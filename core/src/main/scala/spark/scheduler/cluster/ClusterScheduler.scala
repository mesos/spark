package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream}
import java.lang.{Boolean => JBoolean}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark._
import spark.TaskState.TaskState
import spark.scheduler._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

/**
 * The main TaskScheduler implementation, for running tasks on a cluster. Clients should first call
 * start(), then submit task sets through the runTasks method.
 */
private[spark] class ClusterScheduler(val sc: SparkContext)
  extends TaskScheduler
  with Logging {

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = System.getProperty("spark.speculation.interval", "100").toLong
  // How often to revive offers in case there are pending tasks - that is how often to try to get
  // tasks scheduled in case there are nodes available : default 0 is to disable it - to preserve existing behavior
  // Note that this is required due to delayed scheduling due to data locality waits, etc.
  // TODO: rename property ?
  val TASK_REVIVAL_INTERVAL = System.getProperty("spark.tasks.revive.interval", "0").toLong

  /*
   This property controls how aggressive we should be to modulate waiting for host local task scheduling.
   To elaborate, currently there is a time limit (3 sec def) to ensure that spark attempts to wait for host locality of tasks before
   scheduling on other nodes. We have modified this in yarn branch such that offers to task set happen in prioritized order :
   host-local, rack-local and then others
   But once all available host local (and no pref) tasks are scheduled, instead of waiting for 3 sec before
   scheduling to other nodes (which degrades performance for time sensitive tasks and on larger clusters), we can
   modulate that : to also allow rack local nodes or any node. The default is still set to HOST - so that previous behavior is
   maintained. This is to allow tuning the tension between pulling rdd data off node and scheduling computation asap.

   TODO: rename property ? The value is one of
   - HOST_LOCAL (default, no change w.r.t current behavior),
   - RACK_LOCAL and
   - ANY

   Note that this property makes more sense when used in conjugation with spark.tasks.revive.interval > 0 : else it is not very effective.
    */
  val TASK_SCHEDULING_AGGRESSION = TaskLocality.parse(System.getProperty("spark.tasks.schedule.aggression", "HOST_LOCAL"))

  val activeTaskSets = new HashMap[String, TaskSetManager]
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToSlaveId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  // Incrementing Mesos task IDs
  val nextTaskId = new AtomicLong(0)

  // Which hosts in the cluster are alive (contains hostPort's)
  val hostPortsAlive = new HashSet[String]
  val hostToAliveHostPorts = new HashMap[String, HashSet[String]]

  // Which slave IDs we have executors on
  val slaveIdsWithExecutors = new HashSet[String]

  val slaveIdToHostPort = new HashMap[String, String]

  // JAR server, if any JARs were added by the user to the SparkContext
  var jarServer: HttpServer = null

  // URIs of JARs to pass to executor
  var jarUris: String = ""

  // Listener object to pass upcalls into
  var listener: TaskSchedulerListener = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  def initialize(context: SchedulerBackend) {
    backend = context
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (JBoolean.getBoolean("spark.speculation")) {
      new Thread("ClusterScheduler speculation check") {
        setDaemon(true)

        override def run() {
          logInfo("Starting speculative execution thread")
          while (true) {
            try {
              Thread.sleep(SPECULATION_INTERVAL)
            } catch {
              case e: InterruptedException => {}
            }
            checkSpeculatableTasks()
          }
        }
      }.start()
    }


    // Change to always run with some default if TASK_REVIVAL_INTERVAL <= 0 ?
    if (TASK_REVIVAL_INTERVAL > 0) {
      new Thread("ClusterScheduler task offer revival check") {
        setDaemon(true)

        override def run() {
          logInfo("Starting speculative task offer revival thread")
          while (true) {
            try {
              Thread.sleep(TASK_REVIVAL_INTERVAL)
            } catch {
              case e: InterruptedException => {}
            }

            if (hasPendingTasks()) backend.reviveOffers()
          }
        }
      }.start()
    }
  }

  def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = new TaskSetManager(this, taskSet)
      activeTaskSets(taskSet.id) = manager
      activeTaskSetsQueue += manager
      taskSetTaskIds(taskSet.id) = new HashSet[Long]()
    }
    backend.reviveOffers()
  }

  def taskSetFinished(manager: TaskSetManager) {
    this.synchronized {
      activeTaskSets -= manager.taskSet.id
      activeTaskSetsQueue -= manager
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskIdToSlaveId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds.remove(manager.taskSet.id)
    }
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = {
    synchronized {
      SparkEnv.set(sc.env)
      // Mark each slave as alive and remember its hostname
      for (o <- offers) {
        // DEBUG Code
        Utils.checkHostPort(o.hostPort)

        slaveIdToHostPort(o.slaveId) = o.hostPort
        hostPortsAlive += o.hostPort
        hostToAliveHostPorts.getOrElseUpdate(Utils.parseHostPort(o.hostPort)._1, new HashSet[String]).add(o.hostPort)
        slaveGained(o.slaveId, o.hostPort)
      }
      // Build a list of tasks to assign to each slave
      val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
      val availableCpus = offers.map(o => o.cores).toArray
      var launchedTask = false


      for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {

        // Split offers based on host local, rack local and off-rack tasks.
        val hostLocalOffers = new HashMap[String, ArrayBuffer[Int]]()
        val rackLocalOffers = new HashMap[String, ArrayBuffer[Int]]()
        val otherOffers = new HashMap[String, ArrayBuffer[Int]]()

        for (i <- 0 until offers.size) {
          val hostPort = offers(i).hostPort
          // DEBUG code
          Utils.checkHostPort(hostPort)
          val host = Utils.parseHostPort(hostPort)._1
          val numHostLocalTasks =  math.max(0, math.min(manager.numPendingTasksForHost(hostPort), availableCpus(i)))
          if (numHostLocalTasks > 0){
            val list = hostLocalOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            for (j <- 0 until numHostLocalTasks) list += i
          }

          val numRackLocalTasks =  math.max(0,
            // Remove host local tasks (which are also rack local btw !) from this
            math.min(manager.numRackLocalPendingTasksForHost(hostPort) - numHostLocalTasks, availableCpus(i)))
          if (numRackLocalTasks > 0){
            val list = rackLocalOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            for (j <- 0 until numRackLocalTasks) list += i
          }
          if (numHostLocalTasks <= 0 && numRackLocalTasks <= 0){
            // add to others list - spread even this across cluster.
            val list = otherOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            list += i
          }
        }

        val offersPriorityList = new ArrayBuffer[Int](
          hostLocalOffers.size + rackLocalOffers.size + otherOffers.size)
        // First host local, then rack, then others
        val numHostLocalOffers = {
          val hostLocalPriorityList = ClusterScheduler.prioritizeContainers(hostLocalOffers)
          offersPriorityList ++= hostLocalPriorityList
          hostLocalPriorityList.size
        }
        val numRackLocalOffers = {
          val rackLocalPriorityList = ClusterScheduler.prioritizeContainers(rackLocalOffers)
          offersPriorityList ++= rackLocalPriorityList
          rackLocalPriorityList.size
        }
        offersPriorityList ++= ClusterScheduler.prioritizeContainers(otherOffers)

        var lastLoop = false
        val lastLoopIndex = TASK_SCHEDULING_AGGRESSION match {
          case TaskLocality.HOST_LOCAL => numHostLocalOffers
          case TaskLocality.RACK_LOCAL => numRackLocalOffers + numHostLocalOffers
          case TaskLocality.ANY => offersPriorityList.size
        }

        do {
          launchedTask = false
          var loopCount = 0
          for (i <- offersPriorityList) {
            val sid = offers(i).slaveId
            val hostPort = offers(i).hostPort

            // If last loop and within the lastLoopIndex, expand scope - else use null (which will use default/existing)
            val overrideLocality = if (lastLoop && loopCount < lastLoopIndex) TASK_SCHEDULING_AGGRESSION else null

            // If last loop, override waiting for host locality - we scheduled all local tasks already and there might be more available ...
            loopCount += 1


            manager.slaveOffer(sid, hostPort, availableCpus(i), overrideLocality) match {
              case Some(task) =>
                tasks(i) += task
                val tid = task.taskId
                taskIdToTaskSetId(tid) = manager.taskSet.id
                taskSetTaskIds(manager.taskSet.id) += tid
                taskIdToSlaveId(tid) = sid
                slaveIdsWithExecutors += sid
                availableCpus(i) -= 1
                launchedTask = true

              case None => {}
            }
          }
          // Loop once more - when lastLoop = true, then we try to schedule task on all nodes irrespective of
          // data locality (we still go in order of priority : but that would not change anything since
          // if data local tasks had been available, we would have scheduled them already)
          if (lastLoop) {
            // prevent more looping
            launchedTask = false
          } else if (!lastLoop && !launchedTask) {
            // Do this only if TASK_SCHEDULING_AGGRESSION != HOST_LOCAL
            if (TASK_SCHEDULING_AGGRESSION != TaskLocality.HOST_LOCAL) {
              // fudge launchedTask to ensure we loop once more
              launchedTask = true
              // dont loop anymore
              lastLoop = true
            }
          }
        } while (launchedTask)
      }
      return tasks
    }
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var taskSetToUpdate: Option[TaskSetManager] = None
    var failedHostPort: Option[String] = None
    var taskFailed = false
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          val slaveId = taskIdToSlaveId(tid)
          val hostPort = slaveIdToHostPort(slaveId)
          if (hostPortsAlive.contains(hostPort)) {
            // DEBUG Code
            Utils.checkHostPort(hostPort)

            slaveIdsWithExecutors -= slaveId
            hostPortsAlive -= hostPort
            hostToAliveHostPorts.getOrElseUpdate(Utils.parseHostPort(hostPort)._1, new HashSet[String]).remove(hostPort)
            activeTaskSetsQueue.foreach(_.hostLost(hostPort))
            failedHostPort = Some(hostPort)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (activeTaskSets.contains(taskSetId)) {
              //activeTaskSets(taskSetId).statusUpdate(status)
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToSlaveId.remove(tid)
            }
            if (state == TaskState.FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, because that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(tid, state, serializedData)
    }
    if (failedHostPort != None) {
      listener.hostLost(failedHostPort.get)
      backend.reviveOffers()
    }
    if (taskFailed) {
      // Also revive offers if a task had failed for some reason other than host lost
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (activeTaskSets.size > 0) {
        // Have each task set throw a SparkException with the error
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.error(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (jarServer != null) {
      jarServer.stop()
    }

    // sleeping for an arbitrary 5 seconds : to ensure that messages are sent out.
    // TODO: Do something better !
    Thread.sleep(5000L)
  }

  override def defaultParallelism() = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      for (ts <- activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  // Check for pending tasks in all our active jobs.
  def hasPendingTasks(): Boolean = {
    synchronized {
      activeTaskSetsQueue.exists( _.hasPendingTasks() )
    }
  }

  def slaveLost(slaveId: String) {
    var failedHostPort: Option[String] = None
    synchronized {
      val hostPort = slaveIdToHostPort(slaveId)
      if (hostPortsAlive.contains(hostPort)) {
        // DEBUG Code
        Utils.checkHostPort(hostPort)

        slaveIdsWithExecutors -= slaveId
        hostPortsAlive -= hostPort
        hostToAliveHostPorts.getOrElseUpdate(Utils.parseHostPort(hostPort)._1, new HashSet[String]).remove(hostPort)
        activeTaskSetsQueue.foreach(_.hostLost(hostPort))
        failedHostPort = Some(hostPort)
      }
    }
    if (failedHostPort != None) {
      listener.hostLost(failedHostPort.get)
      backend.reviveOffers()
    }
  }

  def slaveGained(slaveId: String, hostPort: String) {
    listener.hostGained(hostPort)
  }


  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  // By default, (cached) hosts for rack is unknown
  def getCachedHostsForRack(rack: String): Option[Set[String]] = None
}



object ClusterScheduler {

  // Used to 'spray' available containers across the available set to ensure too many containers on same host
  // are not used up. Used in yarn mode and in task scheduling (when there are multiple containers available
  // to execute a task)
  // For example: yarn can returns more containers than we would have requested under ANY, this method
  // prioritizes how to use the allocated containers.
  // flatten the map such that the array buffer entries are spread out across the returned value.
  // given <host, list[container]> == <h1, [c1 .. c5]>, <h2, [c1 .. c3]>, <h3, [c1, c2]>, <h4, c1>, <h5, c1>, i
  // the return value would be something like : h1c1, h2c1, h3c1, h4c1, h5c1, h1c2, h2c2, h3c2, h1c3, h2c3, h1c4, h1c5
  // We then 'use' the containers in this order (consuming only the top K from this list where
  // K = number to be user). This is to ensure that if we have multiple eligible allocations,
  // they dont end up allocating all containers on a small number of hosts - increasing probability of
  // multiple container failure when a host goes down.
  // Note, there is bias for keys with higher number of entries in value to be picked first (by design)
  // Also note that invocation of this method is expected to have containers of same 'type'
  // (host-local, rack-local, off-rack) and not across types : so that reordering is simply better from
  // the available list - everything else being same.
  // That is, we we first consume data local, then rack local and finally off rack nodes. So the
  // prioritization from this method applies to within each category
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map.get(left).getOrElse(Set()).size > map.get(right).getOrElse(Set()).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found){
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(null != containerList)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
