package spark

import com.google.common.io.Files
import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Reads events from an event log on disk and processes them.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) {
  val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.arthur.logPath")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)
  val events = new ArrayBuffer[EventLogEntry]
  private val _rdds: ArrayBuffer[RDD[_]] = new ArrayBuffer[RDD[_]]
  loadNewEvents()

  /** List of RDDs from the event log, indexed by their IDs. */
  def rdds = _rdds.readOnly

  /** Prints a human-readable list of RDDs. */
  def printRDDs() {
    for (RDDCreation(rdd, location) <- events) {
      println("#%02d: %-20s %s".format(rdd.id, rddType(rdd), firstExternalElement(location)))
    }
  }

  /** Returns the path of a PDF file containing a visualization of the RDD graph. */
  def visualizeRDDs(): String = {
    val file = File.createTempFile("spark-rdds-", "")
    val dot = new java.io.PrintWriter(file)
    dot.println("digraph {")
    for (RDDCreation(rdd, location) <- events) {
      dot.println("  %d [label=\"%d %s\"]".format(rdd.id, rdd.id, rddType(rdd)))
      for (dep <- rdd.dependencies) {
        dot.println("  %d -> %d;".format(rdd.id, dep.rdd.id))
      }
    }
    dot.println("}")
    dot.close()
    Runtime.getRuntime.exec("dot -Grankdir=BT -Tpdf " + file + " -o " + file + ".pdf")
    file + ".pdf"
  }

  /** List of all tasks. */
  def tasks: Seq[Task[_]] =
    for {
      TaskSubmission(tasks) <- events
      task <- tasks
    } yield task

  /** Reads any new events from the event log. */
  def loadNewEvents() {
    for (ois <- objectInputStream) {
      try {
        while (true) {
          val event = ois.readObject.asInstanceOf[EventLogEntry]
          events += event
          event match {
            case RDDCreation(rdd, location) =>
              sc.updateRddId(rdd.id)
              _rdds += rdd
            case _ => {}
          }
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }

  private def firstExternalElement(location: Array[StackTraceElement]) =
    (location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
      orElse { location.headOption }
      getOrElse { "" })

  private def rddType(rdd: RDD[_]): String =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")
}
