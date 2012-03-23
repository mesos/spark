package spark

import com.google.common.io.Files
import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Reads events from an event log on disk and processes them.
 */
class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) {
  val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.arthur.logPath")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)
  val events = new mutable.ArrayBuffer[EventLogEntry]
  /** List of RDDs indexed by their canonical ID. */
  private val _rdds = new mutable.ArrayBuffer[RDD[_]]
  /** Map of RDD ID to canonical RDD ID (reverse of _rdds). */
  private val rddIdToCanonical = new mutable.HashMap[Int, Int]
  loadNewEvents()

  // Enable checksum verification of loaded RDDs as they are computed
  for (w <- sc.env.eventReporter.eventLogWriter)
    w.enableChecksumVerification(this)

  val checksumMismatches = new mutable.ArrayBuffer[(ChecksumEvent, ChecksumEvent)]

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

  /** Finds the tasks that were run to compute the given RDD. */
  def tasksForRDD(rdd: RDD[_]): Seq[Task[_]] =
    for {
      task <- tasks
      taskRDD <- task match {
        case rt: ResultTask[_, _] => Some(rt.rdd)
        case smt: ShuffleMapTask => Some(smt.rdd)
        case _ => None
      }
      if taskRDD.id == rdd.id
    } yield task

  /** Finds the task for the given stage ID and partition. */
  def taskWithId(stageId: Int, partition: Int): Option[Task[_]] =
    (for {
      task <- tasks
      (taskStageId, taskPartition) <- task match {
        case rt: ResultTask[_, _] => Some((rt.stageId, rt.partition))
        case smt: ShuffleMapTask => Some((smt.stageId, smt.partition))
        case _ => None
      }
      if taskStageId == stageId && taskPartition == partition
    } yield task).headOption

  /**
   * Inserts a lazily-checked element assertion on the specific RDD into the RDD graph. Returns the
   * RDD with the assertion applied.
   *
   * The given RDD, and any RDDs that depend on it, will be replaced. Make sure to get the new
   * version of all RDDs using rdds.
   */
  def assert[T: ClassManifest](rdd: RDD[T], assertion: T => Boolean): RDD[T] = {
    val rddId = rdd.id
    val newRDD = new ElementAssertionRDD(rdd, { (x: T, _: Split) =>
      if (!assertion(x)) Some(ElementAssertionFailure(rddId, x))
      else None
    })
    replace(rdd, newRDD)
    newRDD
  }

  /**
   * Inserts a lazily-checked reduce assertion on the specific RDD into the RDD graph. Returns the
   * RDD with the assertion applied. The reducer operates on each partition independently, and only
   * checks the assertion after the entire partition has been recomputed.
   *
   * The given RDD, and any RDDs that depend on it, will be replaced. Make sure to get the new
   * version of all RDDs using rdds.
   */
  def assert[T: ClassManifest](rdd: RDD[T], reducer: (T, T) => T, assertion: T => Boolean): RDD[T] = {
    // After the given RDD, insert a transformation that checks the assertion
    val rddId = rdd.id
    val newRDD = new ReduceAssertionRDD(rdd, reducer, { (x: T, split: Split) =>
      if (!assertion(x)) Some(ReduceAssertionFailure(rddId, split.index, x))
      else None
    })
    replace(rdd, newRDD)
    newRDD
  }

  /**
   * Runs the specified task locally in a new JVM with the given options, and blocks until the task
   * has completed. While the task is running, it takes over the input and output streams.
   */
  def debugTask(taskStageId: Int, taskPartition: Int, debugOpts: Option[String] = None) {
    for {
      elp <- eventLogPath orElse { Option(System.getProperty("spark.arthur.logPath")) }
      sparkHome <- Option(sc.sparkHome) orElse { Option("") }
      task <- taskWithId(taskStageId, taskPartition)
      (rdd, partition) <- task match {
        case rt: ResultTask[_, _] => Some((rt.rdd, rt.partition))
        case smt: ShuffleMapTask => Some((smt.rdd, smt.partition))
        case _ => None
      }
      debugOptsString <- debugOpts orElse {
        Option("-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000")
      }
    } try {
      val ser = SparkEnv.get.serializer.newInstance()
      val tempDir = Files.createTempDir()
      val file = new File(tempDir, "debugTask-%d-%d".format(taskStageId, taskPartition))
      val out = ser.outputStream(new BufferedOutputStream(new FileOutputStream(file)))
      println("Computing input for task %s into %s".format(task, file))
      val elems = sc.runJob(rdd, (iter: Iterator[_]) => iter.toArray, List(partition), true)
      for (elem <- elems(0)) {
        out.writeObject(elem)
      }
      out.close()

      println("Running task " + task)

      // Launch the task in a separate JVM with debug options set
      val pb = new ProcessBuilder(List(
        "./run", "spark.DebuggingTaskRunner", elp, taskStageId.toString,
        taskPartition.toString, file.getPath, sc.master, sparkHome
      ) ::: sc.jars.toList)
      pb.environment.put("SPARK_DEBUG_OPTS", debugOptsString)
      pb.redirectErrorStream(true)
      val proc = pb.start()

      // Pipe the task's stdout and stderr to our own
      new Thread {
        override def run {
          val procStdout = proc.getInputStream
          var byte: Int = procStdout.read()
          while (byte != -1) {
            System.out.write(byte)
            byte = procStdout.read()
          }
        }
      }.start()
      proc.waitFor()
      println("Finished running task " + task)
    } catch {
      case ex => println("Failed to run task %s: %s".format(task, ex))
    }
  }

  /** Runs the task that caused the specified exception locally. See debugTask. */
  def debugException(event: ExceptionEvent, debugOpts: Option[String] = None) {
    for ((taskStageId, taskPartition) <- event.task match {
      case rt: ResultTask[_, _] => Some((rt.stageId, rt.partition))
      case smt: ShuffleMapTask => Some((smt.stageId, smt.partition))
      case _ => None
    }) {
      debugTask(taskStageId, taskPartition, debugOpts)
    }
  }

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
              rddIdToCanonical(rdd.id) = rdd.id
            case _ => {}
          }
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }

  private[spark] def reportChecksumMismatch(recordedChecksum: ChecksumEvent, newChecksum: ChecksumEvent) {
    checksumMismatches.append((recordedChecksum, newChecksum))
  }

  /** Replaces rdd with newRDD in the dependency graph. */
  private def replace[T: ClassManifest](rdd: RDD[T], newRDD: RDD[T]) {
    val canonicalId = rddIdToCanonical(rdd.id)
    _rdds(canonicalId) = newRDD
    rddIdToCanonical(newRDD.id) = canonicalId

    for (descendantRddIndex <- (canonicalId + 1) until _rdds.length) {
      val updatedRDD = _rdds(descendantRddIndex).mapDependencies(new (RDD ~> RDD) {
        def apply[U](dependency: RDD[U]): RDD[U] = {
          _rdds(rddIdToCanonical(dependency.id)).asInstanceOf[RDD[U]]
        }
      })
      _rdds(descendantRddIndex) = updatedRDD
      rddIdToCanonical(updatedRDD.id) = descendantRddIndex
    }
  }

  private def firstExternalElement(location: Array[StackTraceElement]) =
    (location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
      orElse { location.headOption }
      getOrElse { "" })

  private def rddType(rdd: RDD[_]): String =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")
}
