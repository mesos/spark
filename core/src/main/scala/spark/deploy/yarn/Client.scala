package spark.deploy.yarn

import java.net.{InetSocketAddress, URI}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import spark.{Logging, Utils}
import org.apache.hadoop.yarn.util.{Apps, Records, ConverterUtils}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

class Client(conf: Configuration, args: ClientArguments) extends Logging {
  
  def this(args: ClientArguments) = this(new Configuration(), args)
  
  var applicationsManager: ClientRMProtocol = null
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  
  def run() {
    connectToASM()
    logClusterResourceDetails()

    val newApp = getNewApplication()
    val appId = newApp.getApplicationId()

    verifyClusterResources(newApp)
    val appContext = createApplicationSubmissionContext(appId)
    val localResources = prepareLocalResources(appId, "spark")
    val env = setupLaunchEnv(localResources)
    val amContainer = createContainerLaunchContext(newApp, localResources, env)

    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(args.amUser)
    
    submitApp(appContext)
    
    monitorApplication(appId)
    System.exit(0)
  }
  
  
  def connectToASM() {
    val rmAddress: InetSocketAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS)
    )
    logInfo("Connecting to ResourceManager at" + rmAddress)
    applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf)
      .asInstanceOf[ClientRMProtocol]
  }

  def logClusterResourceDetails() {
    val clusterMetrics: YarnClusterMetrics = getYarnClusterMetrics
    logInfo("Got Cluster metric info from ASM, numNodeManagers=" + clusterMetrics.getNumNodeManagers)

/*
    val clusterNodeReports: List[NodeReport] = getNodeReports
    logDebug("Got Cluster node info from ASM")
    for (node <- clusterNodeReports) {
      logDebug("Got node report from ASM for, nodeId=" + node.getNodeId + ", nodeAddress=" + node.getHttpAddress +
        ", nodeRackName=" + node.getRackName + ", nodeNumContainers=" + node.getNumContainers + ", nodeHealthStatus=" + node.getNodeHealthStatus)
    }
*/

    val queueInfo: QueueInfo = getQueueInfo(args.amQueue)
    logInfo("Queue info .. queueName=" + queueInfo.getQueueName + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity +
      ", queueMaxCapacity=" + queueInfo.getMaximumCapacity + ", queueApplicationCount=" + queueInfo.getApplications.size +
      ", queueChildQueueCount=" + queueInfo.getChildQueues.size)
  }

  def getYarnClusterMetrics: YarnClusterMetrics = {
    val request: GetClusterMetricsRequest = Records.newRecord(classOf[GetClusterMetricsRequest])
    val response: GetClusterMetricsResponse = applicationsManager.getClusterMetrics(request)
    return response.getClusterMetrics
  }

  def getNodeReports: List[NodeReport] = {
    val request: GetClusterNodesRequest = Records.newRecord(classOf[GetClusterNodesRequest])
    val response: GetClusterNodesResponse = applicationsManager.getClusterNodes(request)
    return response.getNodeReports.toList
  }

  def getQueueInfo(queueName: String): QueueInfo = {
    val request: GetQueueInfoRequest = Records.newRecord(classOf[GetQueueInfoRequest])
    request.setQueueName(queueName)
    request.setIncludeApplications(true)
    request.setIncludeChildQueues(false)
    request.setRecursive(false)
    Records.newRecord(classOf[GetQueueInfoRequest])
    return applicationsManager.getQueueInfo(request).getQueueInfo
  }

  def getNewApplication(): GetNewApplicationResponse = {
    logInfo("Requesting new Application")
    val request = Records.newRecord(classOf[GetNewApplicationRequest])
    val response = applicationsManager.getNewApplication(request)
    logInfo("Got new ApplicationId: " + response.getApplicationId())
    return response
  }
  
  def verifyClusterResources(app: GetNewApplicationResponse) = { 
    val maxMem = app.getMaximumResourceCapability().getMemory()
    logInfo("Max mem capabililty of resources in this cluster " + maxMem)
    
    // If the cluster does not have enough memory resources, exit.
    val requestedMem = args.amMemory + args.numWorkers * args.workerMemory
    if (requestedMem > maxMem) {
      logError("Cluster cannot satisfy memory resource request of " + requestedMem)
      System.exit(1)
    }
  }
  
  def createApplicationSubmissionContext(appId: ApplicationId): ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName("Spark")
    return appContext
  }
  
  def prepareLocalResources(appId: ApplicationId, appName: String): HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    // Upload Spark and the application JAR to the remote file system
    // Add them as local resources to the AM
    val fs = FileSystem.get(conf)
    Map("spark.jar" -> System.getenv("SPARK_JAR"), "app.jar" -> args.userJar, "log4j.properties" -> System.getenv("SPARK_LOG4J_CONF"))
    .foreach { case(destName, localPath) =>
      if (null != localPath) {
        val src = new Path(localPath)
        val pathSuffix = appName + "/" + appId.getId() + destName
        val dst = new Path(fs.getHomeDirectory(), pathSuffix)
        logInfo("Uploading " + src + " to " + dst)
        fs.copyFromLocalFile(false, true, src, dst)
        val destStatus = fs.getFileStatus(dst)

        val amJarRsrc = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
        amJarRsrc.setType(LocalResourceType.FILE)
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION)
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst))
        amJarRsrc.setTimestamp(destStatus.getModificationTime())
        amJarRsrc.setSize(destStatus.getLen())
        locaResources(destName) = amJarRsrc
      }
    }
    return locaResources
  }
  
  def setupLaunchEnv(localResources: HashMap[String, LocalResource]): HashMap[String, String] = {
    logInfo("Setting up the launch environment")
    val log4jConfLocalRes = localResources.getOrElse("log4j.properties", null)

    val env = new HashMap[String, String]()
    Apps.addToEnvironment(env, Environment.USER.name, args.amUser)

    // If log4j present, ensure ours overrides all others
    if (null != log4jConfLocalRes) Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./")

    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "$CLASSPATH")
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./*")
    Client.populateHadoopClasspath(yarnConf, env)
    Utils.setYarnMode(env)
    env("SPARK_YARN_JAR_PATH") = 
      localResources("spark.jar").getResource().getScheme.toString() + "://" +
      localResources("spark.jar").getResource().getFile().toString()
    env("SPARK_YARN_JAR_TIMESTAMP") =  localResources("spark.jar").getTimestamp().toString()
    env("SPARK_YARN_JAR_SIZE") =  localResources("spark.jar").getSize().toString()

    env("SPARK_YARN_USERJAR_PATH") =
      localResources("app.jar").getResource().getScheme.toString() + "://" +
      localResources("app.jar").getResource().getFile().toString()
    env("SPARK_YARN_USERJAR_TIMESTAMP") =  localResources("app.jar").getTimestamp().toString()
    env("SPARK_YARN_USERJAR_SIZE") =  localResources("app.jar").getSize().toString()

    if (null != log4jConfLocalRes) {
      env("SPARK_YARN_LOG4J_PATH") =
        log4jConfLocalRes.getResource().getScheme.toString() + "://" + log4jConfLocalRes.getResource().getFile().toString()
      env("SPARK_YARN_LOG4J_TIMESTAMP") =  log4jConfLocalRes.getTimestamp().toString()
      env("SPARK_YARN_LOG4J_SIZE") =  log4jConfLocalRes.getSize().toString()
    }

    // Add each SPARK-* key to the environment
    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
  }

  def userArgsToString(clientArgs: ClientArguments): String = {
    val prefix = " --args "
    val args = clientArgs.userArgs
    val retval = new StringBuilder()
    for (arg <- args){
      retval.append(prefix).append(" '").append(arg).append("' ")
    }

    retval.toString
  }

  def createContainerLaunchContext(newApp: GetNewApplicationResponse,
                                   localResources: HashMap[String, LocalResource],
                                   env: HashMap[String, String]): ContainerLaunchContext = {
    logInfo("Setting up container launch context")
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(env)

    var amMemory = java.lang.Math.max(args.amMemory,
      newApp.getMinimumResourceCapability().getMemory() - YarnAllocationHandler.MEMORY_OVERHEAD)
    
    // Extra options for the JVM
    var JAVA_OPTS = ""

    // Add Xmx for am memory
    JAVA_OPTS += "-Xmx" + amMemory + "m "

    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }

    // Command for the ApplicationMaster
    val commands = List[String]("java " +
      JAVA_OPTS +
      " spark.deploy.yarn.ApplicationMaster" +
      " --class " + args.userClass + 
      " --jar " + args.userJar +
      userArgsToString(args) +
      " --worker-memory " + args.workerMemory +
      " --worker-cores " + args.workerCores +
      " --num-workers " + args.numWorkers +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Command for the ApplicationMaster: " + commands(0))
    amContainer.setCommands(commands)
    
    val capability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // Memory for the ApplicationMaster
    capability.setMemory(args.amMemory)
    amContainer.setResource(capability)
    
    return amContainer
  }
  
  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Create the request to send to the applications manager 
    val appRequest = Records.newRecord(classOf[SubmitApplicationRequest])
      .asInstanceOf[SubmitApplicationRequest]
    appRequest.setApplicationSubmissionContext(appContext)
    // Submit the application to the applications manager
    logInfo("Submitting application to ASM")
    applicationsManager.submitApplication(appRequest)
  }
  
  def monitorApplication(appId: ApplicationId): Boolean = {  
    while(true) {
      Thread.sleep(1000)
      val reportRequest = Records.newRecord(classOf[GetApplicationReportRequest])
        .asInstanceOf[GetApplicationReportRequest]
      reportRequest.setApplicationId(appId)
      val reportResponse = applicationsManager.getApplicationReport(reportRequest)
      val report = reportResponse.getApplicationReport()

      logInfo("Application report from ASM: \n" +
        "\t application identifier: " + appId.toString() + "\n" +
        "\t appId: " + appId.getId() + "\n" +
        "\t clientToken: " + report.getClientToken() + "\n" +
        "\t appDiagnostics: " + report.getDiagnostics() + "\n" +
        "\t appMasterHost: " + report.getHost() + "\n" +
        "\t appQueue: " + report.getQueue() + "\n" +
        "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
        "\t appStartTime: " + report.getStartTime() + "\n" +
        "\t yarnAppState: " + report.getYarnApplicationState() + "\n" +
        "\t distributedFinalState: " + report.getFinalApplicationStatus() + "\n" +
        "\t appTrackingUrl: " + report.getTrackingUrl() + "\n" +
        "\t appUser: " + report.getUser()
      )
      
      val state = report.getYarnApplicationState()
      val dsStatus = report.getFinalApplicationStatus()
      if (state == YarnApplicationState.FINISHED || 
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
          return true
      }
    }
    return true
  }
}

object Client {
  def main(argStrings: Array[String]) {
    val args = new ClientArguments(argStrings)
    Utils.setYarnMode()
    new Client(args).run
  }

  // Based on code from org.apache.hadoop.mapreduce.v2.util.MRApps
  def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String]) {
    for (c <- conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }
}
