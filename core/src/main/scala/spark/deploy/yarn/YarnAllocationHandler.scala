package spark.deploy.yarn

import spark.{Logging, SplitInfo}
import scala.collection
import org.apache.hadoop.yarn.api.records.{AMResponse, ApplicationAttemptId, ContainerId, Priority, Resource, ResourceRequest, ContainerStatus, Container}
import spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.hadoop.yarn.api.protocolrecords.{AllocateRequest, AllocateResponse}
import org.apache.hadoop.yarn.util.{RackResolver, Records}
import java.util.concurrent.{CopyOnWriteArrayList, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.yarn.api.AMRMProtocol
import collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import org.apache.hadoop.conf.Configuration

object AllocationType extends Enumeration ("HOST", "RACK", "ANY") {
  type AllocationType = Value
  val HOST, RACK, ANY = Value
}

// too many params ? refactor it 'somehow' ?
// This class is NOT expected to be MT safe ... though probably it does not need to be I guess ?
// Need to refactor this to make it 'cleaner' ... right now, all computation is reactive : should make it more proactive and decoupled.
// Note that right now, we assume all node asks as uniform in terms of capabilities and priority.
// Particularly the priority will need ot b
private[yarn] class YarnAllocationHandler(val conf: Configuration, val resourceManager : AMRMProtocol, val appAttemptId : ApplicationAttemptId,
                                          val maxWorkers: Int, val workerMemory: Int, val workerCores: Int,
                                          // This and Set's within are immutable - make copies off it to work if required.
                                          val preferredHostToCount: Map[String, Int], val preferredRackToCount: Map[String, Int])
  extends Logging {


  // Both of these are locked on allocatedHostToContainersMap. Complementary data structures
  // allocatedHostToContainersMap : containers which are running : host, Set<containerid>
  // allocatedContainerToHostMap: container to host mapping
  private val allocatedHostToContainersMap : HashMap[String, collection.mutable.Set[ContainerId]] = new HashMap[String, collection.mutable.Set[ContainerId]]()
  private val allocatedContainerToHostMap : HashMap[ContainerId, String] = new HashMap[ContainerId, String]()
  // allocatedRackCount is populated ONLY if allocation happens (or decremented if this is an allocated node)
  // As with the two data structures above, tightly coupled with them, and to be locked on allocatedHostToContainersMap
  private val allocatedRackCount : HashMap[String, Int] = new HashMap[String, Int]()

  // containers which have been released.
  private val releasedContainerList : CopyOnWriteArrayList[ContainerId] = new CopyOnWriteArrayList[ContainerId]()
  // value is irrelevant, we just need a Set actually ...
  private val pendingReleaseContainers : ConcurrentHashMap[ContainerId, Boolean] = new ConcurrentHashMap[ContainerId, Boolean]

  private val numWorkersRunning = new AtomicInteger()
  private val workerIdCounter = new AtomicInteger()
  private val lastResponseId = new AtomicInteger()

  def getNumWorkersRunning : Int = numWorkersRunning.intValue


  def isResourceConstraintSatisfied(container: Container) : Boolean = {
    container.getResource.getMemory >= workerMemory
  }

  // flatten the map such that the array buffer entries are spread out across the returned value
  // Note, there is bias for keys with higher number of entries in value to be picked first (by design)
  def scatterGatherMap(map: HashMap[String, ArrayBuffer[Container]]): List[Container] = {
    val _keyList : ArrayBuffer[String] = new ArrayBuffer[String](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left: String, right: String) => map.get(left).getOrElse(Set()).size > map.get(right).getOrElse(Set()).size
    )

    val retval : ArrayBuffer[Container] = new ArrayBuffer[Container](keyList.size * 2)
    var index : Int = 0
    var found: Boolean = true

    while (found){
      found = false
      for (key : String <- keyList) {
        val containerList : ArrayBuffer[Container] = map.get(key).getOrElse(null)
        assert(null != containerList)
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  def allocateContainers(workersToRequest: Int) {
    // We need to send the request only once from what I understand ... but for now, not modifying this much.

    // Keep polling the Resource Manager for containers
    val amResp : AMResponse = allocateWorkerResources(workersToRequest).getAMResponse

    val _allocatedContainers = amResp.getAllocatedContainers()
    if (_allocatedContainers.size > 0) {


      logInfo("Allocated " + _allocatedContainers.size + " containers, current count " + numWorkersRunning.get() +
        ", to-be-released " + releasedContainerList + ", pendingReleaseContainers : " + pendingReleaseContainers)
      logInfo("Cluster Resources: " + amResp.getAvailableResources)

      val hostToContainers : HashMap[String, ArrayBuffer[Container]] = new HashMap[String, ArrayBuffer[Container]]()

      // ignore if not satisfying constraints      {
      for (container : Container <- _allocatedContainers) {
        if (isResourceConstraintSatisfied(container)) {
          // allocatedContainers += container

          val host : String = container.getNodeId.getHost
          val containers : ArrayBuffer[Container] = hostToContainers.getOrElseUpdate(host, new ArrayBuffer[Container]())

          containers += container
        }
        // Add all ignored containers to released list
        else releasedContainerList.add(container.getId())
      }

      // Find the appropriate containers to use
      // Remove all containers which are not satisfying our requirements
      // Slightly non trivial groupBy I guess ,,,
      var dataLocalContainers : HashMap[String, ArrayBuffer[Container]] = new HashMap[String, ArrayBuffer[Container]]()
      var rackLocalContainers : HashMap[String, ArrayBuffer[Container]] = new HashMap[String, ArrayBuffer[Container]]()
      var offRackContainers : HashMap[String, ArrayBuffer[Container]] = new HashMap[String, ArrayBuffer[Container]]()

      // for ((candidateHost : String, containerList : ArrayBuffer[Container]) <- hostToContainers)
      for (candidateHost <- hostToContainers.keySet)
      {
        val maxExpectedHostCount = preferredHostToCount.getOrElse(candidateHost, 0)
        val requiredHostCount : Int = maxExpectedHostCount - allocatedContainersOnHost(candidateHost)

        // var remainingContainers : ArrayBuffer[Container] = containerList
        var remainingContainers : ArrayBuffer[Container] = hostToContainers.get(candidateHost).getOrElse(null)
        assert(null != remainingContainers)

        if (requiredHostCount >= remainingContainers.size){
          // Add all to dataLocalContainers
          dataLocalContainers.put(candidateHost, remainingContainers)
          // all consumed
          remainingContainers = null
        }
        else if (requiredHostCount > 0) {
          // container list has more containers than we need for data locality.
          // Split into two : data local container count of (remainingContainers.size - requiredHostCount) and rest as remainingContainer
          val (dataLocal, remaining) = remainingContainers.splitAt(remainingContainers.size - requiredHostCount)
          dataLocalContainers.put(candidateHost, dataLocal)
          // remainingContainers = remaining

          // yarn has nasty habit of allocating a tonne of containers on a host - discourage this : add remaining to release list.
          for (container : Container <- remaining) releasedContainerList.add(container.getId())
          remainingContainers = null
        }

        // rack local ?
        if (null != remainingContainers){
          val rack : String = YarnAllocationHandler.lookupRack(conf, candidateHost)

          if (null != rack){
            val maxExpectedRackCount = preferredRackToCount.getOrElse(rack, 0)
            val requiredRackCount : Int = maxExpectedRackCount - allocatedContainersOnRack(rack) - rackLocalContainers.get(rack).getOrElse(List()).size


            if (requiredRackCount >= remainingContainers.size){
              // Add all to dataLocalContainers
              dataLocalContainers.put(rack, remainingContainers)
              // all consumed
              remainingContainers = null
            }
            else if (requiredRackCount > 0) {
              // container list has more containers than we need for data locality.
              // Split into two : data local container count of (remainingContainers.size - requiredRackCount) and rest as remainingContainer
              val (rackLocal, remaining) = remainingContainers.splitAt(remainingContainers.size - requiredRackCount)
              val existingRackLocal : ArrayBuffer[Container] = rackLocalContainers.getOrElseUpdate(rack, new ArrayBuffer[Container]())

              existingRackLocal ++= rackLocal
              remainingContainers = remaining
            }
          }
        }

        if (null != remainingContainers){
          offRackContainers.put(candidateHost, remainingContainers)
        }
      }

      // Now that we have split the containers into various groups, go through them in order : first host local, then rack local and then any.
      // Note that the list we create below tries to ensure that not all containers end up within a host if there are sufficiently large number of
      // hosts/containers.

      val allocatedContainers : ArrayBuffer[Container] = new ArrayBuffer[Container](_allocatedContainers.size)
      allocatedContainers ++= scatterGatherMap(dataLocalContainers)
      allocatedContainers ++= scatterGatherMap(rackLocalContainers)
      allocatedContainers ++= scatterGatherMap(offRackContainers)

      // Run each of the allocated containers
      for (container : Container <- allocatedContainers) {
        val numWorkersRunningNow = numWorkersRunning.incrementAndGet()
        val workerHostname = container.getNodeId.getHost
        val containerId = container.getId

        assert (container.getResource.getMemory >= workerMemory)

        if (numWorkersRunningNow > maxWorkers) {
          logInfo("Ignoring container " + containerId + " at host " + workerHostname + " .. we already have required number of containers")
          releasedContainerList.add(containerId)
          // reset counter back to old value.
          numWorkersRunning.decrementAndGet()
        }
        else {
          // deallocate + allocate can result in reusing id's wrongly !
          val workerId = workerIdCounter.incrementAndGet().toString
          val masterUrl = "akka://spark@%s:%s/user/%s".format(
            System.getProperty("spark.master.host"), System.getProperty("spark.master.port"),
            StandaloneSchedulerBackend.ACTOR_NAME)
          // This must be a hostname and not ip right ?
          // val workerHostname = Utils.getAddressHostName(container.getNodeId().getHost())
          logInfo("launching container on " + containerId + " host " + workerHostname)
          // just to be safe, simply remove it from pendingReleaseContainers. Should not be there, but ..
          pendingReleaseContainers.remove(containerId)

          val rack : String = YarnAllocationHandler.lookupRack(conf, workerHostname)
          allocatedHostToContainersMap.synchronized {
            val containerSet : collection.mutable.Set[ContainerId] =
              allocatedHostToContainersMap.getOrElseUpdate(workerHostname, new HashSet[ContainerId]())

            containerSet += containerId
            allocatedContainerToHostMap.put(containerId, workerHostname)
            if (null != rack) allocatedRackCount.put(rack, allocatedRackCount.getOrElse(rack, 0) + 1)
          }

          new Thread(
            new WorkerRunnable(container, conf, masterUrl, workerId,
              workerHostname, workerMemory, workerCores)
          ).start()
        }
      }
      logInfo("After allocated " + allocatedContainers.size + " containers (orig : " + _allocatedContainers.size + "), current count " + numWorkersRunning.get() +
        ", to-be-released " + releasedContainerList + ", pendingReleaseContainers : " + pendingReleaseContainers)
    }


    val completedContainers : java.util.List[ContainerStatus] = amResp.getCompletedContainersStatuses()
    if (completedContainers.size > 0){
      logInfo("Completed " + completedContainers.size + " containers, current count " + numWorkersRunning.get() +
        ", to-be-released " + releasedContainerList + ", pendingReleaseContainers : " + pendingReleaseContainers)

      for (completedContainer : ContainerStatus <- completedContainers){
        val containerId = completedContainer.getContainerId

        // Was this released by us ? If yes, then simply remove from containerSet and move on.
        if (pendingReleaseContainers.containsKey(containerId)) {
          pendingReleaseContainers.remove(containerId)
        }
        else {
          // simply decrement count - next iteration of ReporterThread will take care of allocating !
          numWorkersRunning.decrementAndGet()
          logInfo("Container completed ? nodeId: " + containerId + ", state " + completedContainer.getState +
            " httpaddress: " + completedContainer.getDiagnostics)
        }

        allocatedHostToContainersMap.synchronized {
          if (allocatedContainerToHostMap.containsKey(containerId)) {
            val host : String = allocatedContainerToHostMap.get(containerId).getOrElse(null)
            assert (null != host)

            val containerSet : collection.mutable.Set[ContainerId] = allocatedHostToContainersMap.get(host).getOrElse(null)
            assert (null != containerSet)

            containerSet -= containerId
            if (containerSet.isEmpty) allocatedHostToContainersMap.remove(host)
            else allocatedHostToContainersMap.update(host, containerSet)

            allocatedContainerToHostMap -= containerId

            // doing this within locked context, sigh ... move to outside ?
            val rack : String = YarnAllocationHandler.lookupRack(conf, host)
            if (null != rack) {
              val rackCount = allocatedRackCount.getOrElse(rack, 0) - 1
              if (rackCount > 0) allocatedRackCount.put(rack, rackCount)
              else allocatedRackCount.remove(rack)
            }
          }
        }
      }
      logInfo("After completed " + completedContainers.size + " containers, current count " + numWorkersRunning.get() +
        ", to-be-released " + releasedContainerList + ", pendingReleaseContainers : " + pendingReleaseContainers)
    }
  }

  def createRackResourceRequests(hostContainers: List[ResourceRequest]) : List[ResourceRequest] = {
    // First generate modified racks and new set of hosts under it : then issue requests
    val rackToCounts : HashMap[String, Int] = new HashMap[String, Int]()

    // Within this lock - used to read/write to the rack related maps too.
    for (container : ResourceRequest <- hostContainers) {
      val candidateHost : String = container.getHostName
      val candidateNumContainers : Int = container.getNumContainers
      assert(YarnAllocationHandler.ANY_HOST != candidateHost)

      val rack : String = YarnAllocationHandler.lookupRack(conf, candidateHost)
      if (null != rack) {
        var count : Int = rackToCounts.getOrElse(rack, 0)
        count += candidateNumContainers
        rackToCounts.put(rack, count)
      }
    }

    val requestedContainers: ArrayBuffer[ResourceRequest] = new ArrayBuffer[ResourceRequest](rackToCounts.size)
    for ((rack, count) <- rackToCounts){
      requestedContainers ++= createResourceRequest(AllocationType.RACK, rack, count, YarnAllocationHandler.PRIORITY)
    }

    requestedContainers.toList
  }

  def allocatedContainersOnHost(host: String): Int = {
    var retval : Int = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedHostToContainersMap.getOrElse(host, Set()).size
    }
    retval
  }

  def allocatedContainersOnRack(rack: String): Int = {
    var retval : Int = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedRackCount.getOrElse(rack, 0)
    }
    retval
  }

  private def allocateWorkerResources(numWorkers: Int) : AllocateResponse = {

    var resourceRequests : List[ResourceRequest] = null

      // default.
    if (numWorkers <= 0 || preferredHostToCount.isEmpty) {
      val requestedContainers: List[ResourceRequest] = createResourceRequest(AllocationType.ANY, null, numWorkers, YarnAllocationHandler.PRIORITY)
      logInfo("Condition 1 numWorkers: " + numWorkers)
      resourceRequests = requestedContainers
    }
    else {
      // request for all hosts in preferred nodes and for numWorkers - candidates.size, request by default allocation policy.
      val hostContainerRequests: ArrayBuffer[ResourceRequest] = new ArrayBuffer[ResourceRequest](preferredHostToCount.size)
      for ((candidateHost: String, candidateCount: Int) <- preferredHostToCount) {
        val requiredCount = candidateCount - allocatedContainersOnHost(candidateHost)

        if (requiredCount > 0) hostContainerRequests ++= createResourceRequest(AllocationType.HOST, candidateHost, requiredCount, YarnAllocationHandler.PRIORITY)
      }
      val rackContainerRequests: List[ResourceRequest] = createRackResourceRequests(hostContainerRequests.toList)

      val anyContainerRequests: List[ResourceRequest] = createResourceRequest(AllocationType.ANY, null, numWorkers, YarnAllocationHandler.PRIORITY)

      val containerRequests: ArrayBuffer[ResourceRequest] =
        new ArrayBuffer[ResourceRequest](hostContainerRequests.size + rackContainerRequests.size + anyContainerRequests.size)

      containerRequests ++= hostContainerRequests
      containerRequests ++= rackContainerRequests
      containerRequests ++= anyContainerRequests

      resourceRequests = containerRequests.toList
    }

    val req = Records.newRecord(classOf[AllocateRequest])
    req.setResponseId(lastResponseId.incrementAndGet)
    req.setApplicationAttemptId(appAttemptId)

    req.addAllAsks(resourceRequests)

    val releasedContainerList = createReleasedContainerList()
    req.addAllReleases(releasedContainerList)



    // This is just continuation of earlier code, but we should not need to keep requesting if there is no change in 'Ask' right ?
    // Should we add check for it ?

    if (numWorkers > 0) {
      logInfo("Allocating " + numWorkers + " worker containers with " + workerMemory + " of memory each.")
    }
    else {
      logInfo("Empty allocation req ..  release : " + releasedContainerList)
    }

    // debug
    for (req : ResourceRequest <- resourceRequests) logInfo("rsrcRequest ... " + asString(req))
    resourceManager.allocate(req)
  }

  def asString(request: ResourceRequest): String = {
    "host : " + request.getHostName + ", numContainers : " + request.getNumContainers + ", p = " + request.getPriority().getPriority +
      ", priority : " + request.getPriority().toString() +", capability: "  + request.getCapability
  }


  private def createResourceRequest(requestType: AllocationType.AllocationType, resource:String, numWorkers : Int, priority: Int) : List[ResourceRequest] = {

    // If hostname specified, we need atleast two requests - node local and rack local.
    // There must be a third request - which is ANY : that will be specially handled.
    requestType match {
      case AllocationType.HOST => {
        assert (YarnAllocationHandler.ANY_HOST != resource)

        val hostname = resource
        val nodeLocal : ResourceRequest = createResourceRequestImpl(hostname, numWorkers, priority)

        // add to host->rack mapping
        YarnAllocationHandler.populateRackInfo(conf, hostname)

        List(nodeLocal)
      }

      case AllocationType.RACK => {
        val rack = resource
        List(createResourceRequestImpl(rack, numWorkers, priority))
      }

      case AllocationType.ANY => {
        List(createResourceRequestImpl(YarnAllocationHandler.ANY_HOST, numWorkers, priority))
      }

      case _ => throw new IllegalArgumentException("Unexpected/unsupported request type .. " + requestType)
    }
  }

  private def createResourceRequestImpl(hostname:String, numWorkers : Int, priority: Int) : ResourceRequest = {

    val rsrcRequest = Records.newRecord(classOf[ResourceRequest])
    val memCapability = Records.newRecord(classOf[Resource])
    // There probably is some overhead here, let's reserve a bit more memory.
    memCapability.setMemory(workerMemory)
    rsrcRequest.setCapability(memCapability)

    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(priority)
    rsrcRequest.setPriority(pri)

    rsrcRequest.setHostName(hostname)

    rsrcRequest.setNumContainers(java.lang.Math.max(numWorkers, 0))
    rsrcRequest
  }

  def createReleasedContainerList() : ArrayBuffer[ContainerId] = {

    val retval : ArrayBuffer[ContainerId] = new ArrayBuffer[ContainerId](1)
    // iterator on COW list ...
    for (container : ContainerId <- releasedContainerList.iterator()){
      retval += container
    }
    // remove from the original list.
    if (! retval.isEmpty) {
      releasedContainerList.removeAll(retval)
      for (v <- retval) pendingReleaseContainers.put(v, true)
      logInfo("Releasing " + retval.size + " containers. pendingReleaseContainers : " + pendingReleaseContainers)
    }

    retval
  }
}

object YarnAllocationHandler {

  val ANY_HOST = "*"
  // all requests are issued with same priority : we do not (yet) have any distinction between request types (like map/reduce in hadoop for example)
  val PRIORITY = 1
  // Additional memory overhead
  val MEMORY_OVERHEAD = 128

  // host to rack map - saved from allocation requests
  // We are expecting this not to change.
  // Note that it is possible for this to change : and RM will indicate that to us via update response to allocate.
  // But we are punting on handling that for now.
  private val hostToRack : ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()

  def newAllocator(conf: Configuration,
                   resourceManager : AMRMProtocol, appAttemptId : ApplicationAttemptId,
                   args: ApplicationMasterArguments,
                   map: collection.Map[String, collection.Set[SplitInfo]]): YarnAllocationHandler = {

    val (hostToCount, rackToCount) = generateNodeToWeight(conf, map)


    new YarnAllocationHandler(conf, resourceManager, appAttemptId, args.numWorkers, args.workerMemory + MEMORY_OVERHEAD,
      args.workerCores, hostToCount, rackToCount)
  }

  def newAllocator(conf: Configuration,
                   resourceManager : AMRMProtocol, appAttemptId : ApplicationAttemptId,
                   maxWorkers: Int, workerMemory: Int, workerCores: Int,
                   map: collection.Map[String, collection.Set[SplitInfo]]): YarnAllocationHandler = {

    val (hostToCount, rackToCount) = generateNodeToWeight(conf, map)

    new YarnAllocationHandler(conf, resourceManager, appAttemptId, maxWorkers, workerMemory + MEMORY_OVERHEAD,
      workerCores, hostToCount, rackToCount)
  }

  // A simple method to copy the split info map.
  private def generateNodeToWeight(conf: Configuration,
                                   input: scala.collection.Map[String, scala.collection.Set[SplitInfo]]) :
  // host to count, rack to count
  (Map[String, Int], Map[String, Int]) = {

    if (null == input) return (Map[String, Int](), Map[String, Int]())

    val hostToCount : HashMap[String, Int] = new HashMap[String, Int]
    val rackToCount : HashMap[String, Int] = new HashMap[String, Int]

    for ((host: String, splits: scala.collection.Set[SplitInfo]) <- input) {
      val hostCount : Int = hostToCount.getOrElse(host, 0)
      hostToCount.put(host, hostCount + splits.size)

      val rack : String = lookupRack(conf, host)
      if (null != rack){
        val rackCount : Int = rackToCount.getOrElse(host, 0)
        rackToCount.put(host, rackCount + splits.size)
      }
    }

    (hostToCount.toMap, rackToCount.toMap)
  }

  def lookupRack(conf: Configuration, host: String): String = {
    if (! hostToRack.contains(host)) populateRackInfo(conf, host)
    hostToRack.get(host)
  }

  def populateRackInfo(conf: Configuration, hostname: String) {
    if (!hostToRack.containsKey(hostname)) {
      // If there are repeated failures to resolve, all to an ignore list ?
      val rackInfo = RackResolver.resolve(conf, hostname)
      if (null != rackInfo && null != rackInfo.getNetworkLocation) {
        hostToRack.put(hostname, rackInfo.getNetworkLocation)
      }
    }
  }
}
