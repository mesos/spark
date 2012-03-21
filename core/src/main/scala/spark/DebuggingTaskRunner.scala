package spark

import com.google.common.io.Files
import java.io._

/**
 * Loads the specified task from the event log and runs it locally, passing it the given input
 * partition.
 */
object DebuggingTaskRunner {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: DebuggingTaskRunner <eventLogPath> " +
                         "<taskStageId> <taskPartition> <inputPath> " +
                         "<host> [<sparkHome> [<JAR path> ...]]")
      System.exit(1)
    }

    val eventLogPath = args(0)
    val taskStageId = args(1).toInt
    val taskPartition = args(2).toInt
    val inputPath = args(3)
    val host = args(4)
    val sparkHome = if (args.length > 5) Option(args(5)) else None
    val jars = args.slice(6, args.length)

    val sc = new SparkContext(
      host, "Task %d, %d".format(taskStageId, taskPartition),
      sparkHome match {
        case Some(x) => x
        case None => null
      }, jars)

    val r = new EventLogReader(sc, Some(eventLogPath))

    r.taskWithId(taskStageId, taskPartition) match {
      case Some(task) =>
        val ser = SparkEnv.get.serializer.newInstance()
        val file = new File(inputPath)
        val stream =
          ser.inputStream(new BufferedInputStream(new FileInputStream(file)))
        val iterator = new DeserializingIterator(stream)
        task.run(0, iterator)
        System.exit(0)
      case None =>
        System.err.println(
          "Task ID (%d, %d) is not associated with a task!".format(
            taskStageId, taskPartition))
        System.exit(1)
    }
  }
}

/**
 * Wraps a DeserializationStream into an Iterator.
 */
class DeserializingIterator(stream: DeserializationStream) extends Iterator[Any] {
  def hasNext = !nextObject.isEmpty || readNext()
  def next(): Any =
    if (hasNext) {
      val obj = nextObject.get
      nextObject = None
      obj
    } else {
      throw new UnsupportedOperationException("no more elements")
    }

  private var nextObject: Option[Any] = None
  private def readNext(): Boolean = {
    // Precondition: nextObject should be None
    try {
      nextObject = Option(stream.readObject())
      !nextObject.isEmpty
    } catch {
      case _: EOFException =>
        nextObject = None
        false
    }
  }
}
