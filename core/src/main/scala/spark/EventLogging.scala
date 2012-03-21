package spark

import java.io._

/**
 * Spark program event that can be logged during execution and later replayed using Arthur.
 */
sealed trait EventLogEntry

case class ExceptionEvent(exception: Throwable, task: Task[_]) extends EventLogEntry
case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class TaskSubmission(tasks: Seq[Task[_]]) extends EventLogEntry

sealed trait ChecksumEvent extends EventLogEntry {
  def mismatch(other: ChecksumEvent): Boolean
}

case class TaskChecksum(tid: Int, checksum: Int) extends ChecksumEvent {
  override def mismatch(other: ChecksumEvent) = other match {
    case TaskChecksum(otherTid, otherChecksum) => tid == otherTid && checksum != otherChecksum
    case _ => false
  }
}

case class ShuffleChecksum(
  rddId: Int,
  shuffleId: Int,
  partition: Int,
  outputSplit: Int,
  checksum: Int
) extends ChecksumEvent {
  override def mismatch(other: ChecksumEvent) = other match {
    case ShuffleChecksum(a, b, c, d, otherChecksum) =>
      (rddId, shuffleId, partition, outputSplit) == (a, b, c, d) && checksum != otherChecksum
    case _ =>
      false
  }
}

sealed trait AssertionFailure extends EventLogEntry
case class ElementAssertionFailure(rddId: Int, element: Any) extends AssertionFailure
case class ReduceAssertionFailure(rddId: Int, splitIndex: Int, element: Any) extends AssertionFailure

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
