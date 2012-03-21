package spark

import java.io._

/**
 * Spark program event that can be logged during execution and later replayed using Arthur.
 */
sealed trait EventLogEntry

case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class TaskSubmission(tasks: Seq[Task[_]]) extends EventLogEntry

/**
 * Stream for writing the event log.
 *
 * Certain classes may need to write certain transient fields to the event log. Such classes should
 * implement a special writeObject method (see java.io.Serializable) that takes a
 * java.io.ObjectOutputStream as an argument. They should check the argument's dynamic type; if it
 * is an EventLogOutputStream, they should write the extra fields.
 */
class EventLogOutputStream(out: OutputStream) extends ObjectOutputStream(out)

/**
 * Stream for reading the event log. See EventLogOutputStream.
 */
class EventLogInputStream(in: InputStream, val sc: SparkContext) extends ObjectInputStream(in) {
  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
}
