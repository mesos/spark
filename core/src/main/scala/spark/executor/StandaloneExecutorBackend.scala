package spark.executor

import java.nio.ByteBuffer
import spark.Logging
import spark.TaskState.TaskState
import spark.util.AkkaUtils
import akka.actor.{ActorRef, Actor, ActorSystem, Props, Terminated}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import java.util.concurrent.{TimeUnit, ThreadPoolExecutor, SynchronousQueue}
import spark.scheduler.cluster._
import spark.scheduler.cluster.RegisteredExecutor
import spark.scheduler.cluster.LaunchTask
import spark.scheduler.cluster.RegisterExecutorFailed
import spark.scheduler.cluster.RegisterExecutor
import spark.scheduler.cluster.StopExecutor

private[spark] class StandaloneExecutorBackend(
    driverUrl: String,
    workerUrl: Option[String], // If given, used to detect when the worker wants us to terminate
    executorId: String,
    hostname: String,
    cores: Int,
    actorSystem: ActorSystem)
  extends Actor
  with ExecutorBackend
  with Logging {

  var executor: Executor = null
  var driver: ActorRef = null

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorFor(driverUrl)
    driver ! RegisterExecutor(executorId, hostname, cores)
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    context.watch(driver) // Doesn't work with remote actors, but useful for testing
    // If a worker URL is passed, open a connection to it so that (1) it can tell us when to
    // terminate and (2) we can exit ourselves if the worker process crashes
    for (url <- workerUrl) {
      val worker = context.actorFor(url)
      worker ! RegisterExecutor(executorId, hostname, cores)
      context.watch(worker) // Doesn't work with remote actors, but useful for testing
    }
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with driver")
      executor = new Executor(executorId, hostname, sparkProperties)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      if (executor == null) {
        logError("Received launchTask but executor was null")
        System.exit(1)
      } else {
        executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
      }
      
    case StopExecutor =>
      logInfo("Asked to stop executor")
      sender ! true
      context.stop(self)
      System.exit(0)

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      logError("Driver or worker disconnected! Shutting down.")
      System.exit(1)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object StandaloneExecutorBackend {
  def run(driver: String, worker: Option[String], execId: String, hostname: String, cores: Int) {
    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(driver, worker, execId, hostname, cores, actorSystem)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      //the reason we allow the last frameworkId argument is to make it easy to kill rogue executors
      System.err.println("Usage: StandaloneExecutorBackend " +
        "<driverUrl> <workerUrl> <executorId> <hostname> <cores> [<appid>]")
      System.exit(1)
    }
    val workerUrl = if (args(1) == "none") None else Some(args(1))
    run(args(0), workerUrl, args(2), args(3), args(4).toInt)
  }
}
