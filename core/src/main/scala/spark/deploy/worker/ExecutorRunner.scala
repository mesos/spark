package spark.deploy.worker

import java.io._
import java.net.{URI, URL}

import scala.Some

import akka.actor._
import akka.util.duration._
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

import spark.{Utils, Logging}
import spark.deploy.{ExecutorState, ExecutorStateChanged, ApplicationDescription}
import spark.deploy.ExecutorStateChanged
import spark.scheduler.cluster.RegisterExecutor
import spark.scheduler.cluster.StopExecutor

/**
 * Manages the execution of one executor process.
 */
private[spark] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: ActorRef,
    val workerId: String,
    val hostname: String,
    val sparkHome: File,
    val workDir: File,
    val actorSystem: ActorSystem,
    val actorSystemUrl: String)
  extends Logging {

  val fullId = appId + "/" + execId
  var workerThread: Thread = null
  var process: Process = null
  var shutdownHook: Thread = null
  var watcherActor: ActorRef = null
  var executorActor: ActorRef = null

  // A simple actor that will be used to register the executor actor with us and tell it to stop
  class WatcherActor extends Actor {
    override def receive = {
      case RegisterExecutor(_, _, _) =>
        logInfo("Executor " + fullId + " registered at " + sender.path)
        executorActor = sender

      case "stop" =>
        context.stop(self)
    }
  }

  def start() {
    watcherActor = actorSystem.actorOf(Props(new WatcherActor),
      name = "Watcher_%s_%s".format(appId, execId))

    // Add a shutdown hook that kills executor on shutdown.
    shutdownHook = new Thread() { 
      override def run() {
        logInfo("Shutdown hook killing executor " + fullId)
        stopProcess()
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
  }

  /** Stop this executor runner, including killing the process it launched */
  def kill() {
    if (workerThread != null && workerThread.isAlive) {
      workerThread.interrupt()
      workerThread = null
      stopProcess()
      worker ! ExecutorStateChanged(appId, execId, ExecutorState.KILLED, None, None)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }

  /** Stop the worker process */
  def stopProcess() {
    if (process != null) {
      if (executorActor != null) {
        // Send the actor a message to stop. This doesn't matter on UNIX but does on Windows
        // because Process.destroy will only the top-level "spark-executor" script that spawned
        // an executor JVM. It's not a perfect solution but we'll deal with it by spawning
        // "java" directly in future versions.
        executorActor ! StopExecutor
      }
      process.destroy()
      process.waitFor()
      watcherActor ! "stop"
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{WORKER_URL}}" => "%s/user/Watcher_%s_%s".format(actorSystemUrl, appId, execId)
    case "{{HOSTNAME}}" => hostname
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def buildCommandSeq(): Seq[String] = {
    val command = appDesc.command
    val script = if (System.getProperty("os.name").startsWith("Windows")) "run.cmd" else "run"
    val runScript = new File(sparkHome, script).getCanonicalPath
    Seq(runScript, command.mainClass) ++ (command.arguments ++ Seq(appId)).map(substituteVariables)
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file)
    new Thread("redirect output to " + file) {
      override def run() {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   */
  def fetchAndRunExecutor() {
    try {
      // Create the executor's working directory
      val executorDir = new File(workDir, appId + "/" + execId)
      if (!executorDir.mkdirs()) {
        throw new IOException("Failed to create directory " + executorDir)
      }

      // Launch the process
      val command = buildCommandSeq()
      val builder = new ProcessBuilder(command: _*).directory(executorDir)
      val env = builder.environment()
      for ((key, value) <- appDesc.command.environment) {
        env.put(key, value)
      }
      env.put("SPARK_MEM", memory.toString + "m")
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      env.put("SPARK_LAUNCH_WITH_SCALA", "0")
      process = builder.start()

      // Redirect its stdout and stderr to files
      redirectStream(process.getInputStream, new File(executorDir, "stdout"))
      redirectStream(process.getErrorStream, new File(executorDir, "stderr"))

      // Wait for it to exit; this is actually a bad thing if it happens, because we expect to run
      // long-lived processes only. However, in the future, we might restart the executor a few
      // times on the same machine.
      val exitCode = process.waitFor()
      val message = "Command exited with code " + exitCode
      if (exitCode == 0) {
          // We explicitly shut down with exit code 0 in StandaloneExecutorBackend
          worker ! ExecutorStateChanged(appId, execId, ExecutorState.KILLED, Some(message), Some(exitCode))
      } else {
          worker ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, Some(message), Some(exitCode))
      }
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")

      case e: Exception => {
        logError("Error running executor", e)
        stopProcess()
        val message = e.getClass + ": " + e.getMessage
        worker ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, Some(message), None)
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
      }
    }
  }
}
