package spark

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import java.io._

sealed trait EventReporterMessage
case class LogEvent(entry: EventLogEntry) extends EventReporterMessage
case class StopEventReporter() extends EventReporterMessage

class EventReporterActor(eventLogWriter: EventLogWriter) extends DaemonActor with Logging {
  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('EventReporterActor, self)
    logInfo("Registered actor on port " + port)

    loop {
      react {
        case LogEvent(entry) =>
          eventLogWriter.log(entry)
        case StopEventReporter =>
          eventLogWriter.stop()
          reply('OK)
      }
    }
  }
}

/**
 * Manages event reporting on the master and slaves.
 */
class EventReporter(isMaster: Boolean) extends Logging {
  var enableArthur = System.getProperty("spark.arthur.enabled", "true").toBoolean
  var enableChecksumming = System.getProperty("spark.arthur.checksum", "true").toBoolean
  val host = System.getProperty("spark.master.host")

  var eventLogWriter: Option[EventLogWriter] = None
  /** Remote reference to the actor on workers. */
  var reporterActor: AbstractActor = null
  init()

  def init() {
    eventLogWriter =
      if (isMaster && enableArthur) Some(new EventLogWriter)
      else None
    if (enableArthur) {
      if (isMaster) {
        for (elw <- eventLogWriter) {
          val actor = new EventReporterActor(elw)
          actor.start()
          reporterActor = actor
        }
      } else {
        val host = System.getProperty("spark.master.host")
        val port = System.getProperty("spark.master.port").toInt
        reporterActor = RemoteActor.select(Node(host, port), 'EventReporterActor)
      }
    }
  }

  /** Reports the failure of an RDD assertion. */
  def reportAssertionFailure(failure: AssertionFailure) {
    reporterActor !! LogEvent(failure)
  }

  /** Reports an exception when running a task on a slave. */
  def reportException(exception: Throwable, task: Task[_]) {
    // TODO: The task may refer to an RDD, so sending it through the actor will interfere with RDD
    // back-referencing, causing a duplicate version of the referenced RDD to be serialized. If
    // tasks had IDs, we could just send those.
    reporterActor !! LogEvent(ExceptionEvent(exception, task))
  }

  /**
   * Reports an exception when running a task locally using LocalScheduler. Can only be called on
   * the master.
   */
  def reportLocalException(exception: Throwable, task: Task[_]) {
    for (elw <- eventLogWriter) {
      elw.log(ExceptionEvent(exception, task))
    }
  }

  /** Reports the creation of an RDD. Can only be called on the master. */
  def reportRDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) {
    for (elw <- eventLogWriter) {
      elw.log(RDDCreation(rdd, location))
    }
  }

  /** Reports the creation of a task. Can only be called on the master. */
  def reportTaskSubmission(tasks: Seq[Task[_]]) {
    for (elw <- eventLogWriter) {
      elw.log(TaskSubmission(tasks))
    }
  }

  /** Reports the checksum of a task's results. */
  def reportTaskChecksum(tid: Int, checksum: Int) {
    reporterActor !! LogEvent(TaskChecksum(tid, checksum))
  }

  /** Reports the checksum of a shuffle output. */
  def reportShuffleChecksum(rdd: RDD[_], shuffleId: Int, partition: Int, outputSplit: Int, checksum: Int) {
    reporterActor !! LogEvent(ShuffleChecksum(rdd.id, shuffleId, partition, outputSplit, checksum))
  }

  def stop() {
    reporterActor !? StopEventReporter
    eventLogWriter = None
    reporterActor = null
  }
}
