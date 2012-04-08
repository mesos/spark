package spark

import com.google.common.io.Files
import java.io._
import spark.broadcast._

/**
 * Loads the specified task from the event log and runs it locally.
 */
object DebuggingTaskRunner {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: DebuggingTaskRunner <taskPath>")
      System.exit(1)
    }

    val taskPath = args(0)

    // Initialize the environment similar to how the Mesos executor does
    val env = SparkEnv.createFromSystemProperties(false)
    SparkEnv.set(env)
    Broadcast.initialize(false)

    // Deserialize the task
    val file = new File(taskPath)
    val ser = env.serializer.newInstance()
    val in = ser.inputStream(new BufferedInputStream(new FileInputStream(file)))
    val task = in.readObject[Task[_]]()
    in.close()

    // Run the task
    task.run(0)

    System.exit(0)
  }
}
