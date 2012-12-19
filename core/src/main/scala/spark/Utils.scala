package spark

import java.io._
import java.net.{InetAddress, URL, URI}
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FileUtil}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Various utility methods used by Spark.
 */
private object Utils extends Logging {
  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    return bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /** Split a string into words at non-alphabetic characters */
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j)
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  /** Create a temporary directory inside the given parent directory */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory after " + maxAttempts +
            " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        Utils.deleteRecursively(dir)
      }
    })
    return dir
  }

  /** Copy all data from an InputStream to an OutputStream */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  /** Copy a file on the local file system */
  def copyFile(source: File, dest: File) {
    val in = new FileInputStream(source)
    val out = new FileOutputStream(dest)
    copyStream(in, out, true)
  }

  /** Download a file from a given URL to the local filesystem */
  def downloadFile(url: URL, localPath: String) {
    val in = url.openStream()
    val out = new FileOutputStream(localPath)
    Utils.copyStream(in, out, true)
  }

  /**
   * Download a file requested by the executor. Supports fetching the file in a variety of ways,
   * including HTTP, HDFS and files on a standard filesystem, based on the URL parameter.
   */
  def fetchFile(url: String, targetDir: File) {
    val filename = url.split("/").last
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    uri.getScheme match {
      case "http" | "https" | "ftp" =>
        logInfo("Fetching " + url + " to " + targetFile)
        val in = new URL(url).openStream()
        val out = new FileOutputStream(targetFile)
        Utils.copyStream(in, out, true)
      case "file" | null =>
        // Remove the file if it already exists
        targetFile.delete()
        // Symlink the file locally.
        if (uri.isAbsolute) {
          // url is absolute, i.e. it starts with "file:///". Extract the source
          // file's absolute path from the url.
          val sourceFile = new File(uri)
          logInfo("Symlinking " + sourceFile.getAbsolutePath + " to " + targetFile.getAbsolutePath)
          FileUtil.symLink(sourceFile.getAbsolutePath, targetFile.getAbsolutePath)
        } else {
          // url is not absolute, i.e. itself is the path to the source file.
          logInfo("Symlinking " + url + " to " + targetFile.getAbsolutePath)
          FileUtil.symLink(url, targetFile.getAbsolutePath)
        }
      case _ =>
        // Use the Hadoop filesystem library, which supports file://, hdfs://, s3://, and others
        val uri = new URI(url)
        val conf = new Configuration()
        val fs = FileSystem.get(uri, conf)
        val in = fs.open(new Path(uri))
        val out = new FileOutputStream(targetFile)
        Utils.copyStream(in, out, true)
    }
    // Decompress the file if it's a .tar or .tar.gz
    if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xzf", filename), targetDir)
    } else if (filename.endsWith(".tar")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xf", filename), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(filename, "a+x")
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassManifest](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  def localIpAddress(): String = InetAddress.getLocalHost.getHostAddress

  private var customHostname: Option[String] = None

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(InetAddress.getLocalHost.getHostName)
  }

  // expensive ? cache ?
  def getAddressHostName(address: String) : String = {
    InetAddress.getByName(address).getHostName
  }



  def localHostPort(): String = {
    val retval = System.getProperty("spark.hostname", null)
    if (null == retval) {
      logErrorWithStack("spark.hostname not set but invoking localHostPort")
      return localHostName()
    }

    retval
  }

  def checkHost(host : String, message : String = "") {
    // Currently catches only ipv4 pattern, this is just a debugging tool - not rigourous !
    if (host.matches("^[0-9]+(\\.[0-9]+)*$")) {
      Utils.logErrorWithStack("Unexpected to have host " + host + " which matches IP pattern. Message " + message)
    }
    if (Utils.parseHostPort(host)._2 != 0){
      Utils.logErrorWithStack("Unexpected to have host " + host + " which has port in it. Message " + message)
    }
  }

  def checkHostPort(hostPort : String, message : String = "") {
    val (host, port) = Utils.parseHostPort(hostPort)
    // Currently catches only ipv4 pattern, this is just a debugging tool - not rigourous !
    if (host.matches("^[0-9]+(\\.[0-9]+)*$")) {
      Utils.logErrorWithStack("Unexpected to have host " + host + " which matches IP pattern in " + hostPort + ". Message " + message)
    }
    if (port <= 0){
      Utils.logErrorWithStack("Unexpected to have port " + port + " which is not valid in " + hostPort + ". Message " + message)
    }
  }

  def getUserNameFromEnvironment() : String = {
    // defaulting to env if -D is not present ...
    System.getProperty(Environment.USER.name, System.getenv(Environment.USER.name))
  }

  def logErrorWithStack(msg: String) {
    try { throw new Exception } catch { case ex: Exception => { logError(msg, ex) } }
  }

  // TODO: Cache results ?
  def parseHostPort(hostPort: String) : (String,  Int) = {
    val indx: Int = hostPort.lastIndexOf(':')
    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ... but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    if (-1 == indx) return (hostPort, 0)

    (hostPort.substring(0, indx).trim(), hostPort.substring(indx + 1).trim().toInt)
  }

  def addIfNoPort(hostPort: String,  port: Int) : String = {
    if (port <= 0) throw new IllegalArgumentException("Invalid port specified " + port);

    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ... but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    val indx: Int = hostPort.lastIndexOf(':')
    if (-1 != indx) return hostPort

    hostPort + ":" + port
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn mode, this MUST be set to true.
  def isYarnMode() : Boolean = {
    val yarnMode = System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE"))
    java.lang.Boolean.valueOf(yarnMode)
  }

  // Set an env variable indicating we are running in YARN mode.
  // Note that anything with SPARK prefix gets propagated to all (remote) processes
  def setYarnMode() {
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  def setYarnMode(env : HashMap[String, String]) {
    env("SPARK_YARN_MODE") = "true"
  }

  // Used to 'spray' available containers across the available set to ensure too many containers on same host
  // are not used up. Used in yarn mode and in task scheduling (when there are multiple containers available
  // to execute a task)
  // For example: yarn can returns more containers than we would have requested under ANY, this method
  // prioritizes how to use the allocated containers.
  // flatten the map such that the array buffer entries are spread out across the returned value.
  // given <host, list[container]> == <h1, [c1 .. c5]>, <h2, [c1 .. c3]>, <h3, [c1, c2]>, <h4, c1>, <h5, c1>, i
  // the return value would be something like : h1c1, h2c1, h3c1, h4c1, h5c1, h1c2, h2c2, h3c2, h1c3, h2c3, h1c4, h1c5
  // We then 'use' the containers in this order (consuming only the top K from this list where
  // K = number to be user). This is to ensure that if we have multiple eligible allocations,
  // they dont end up allocating all containers on a small number of hosts - increasing probability of
  // multiple container failure when a host goes down.
  // Note, there is bias for keys with higher number of entries in value to be picked first (by design)
  // Also note that invocation of this method is expected to have containers of same 'type' 
  // (host-local, rack-local, off-rack) and not across types : so that reordering is simply better from 
  // the available list - everything else being same.
  // That is, we we first consume data local, then rack local and finally off rack nodes. So the 
  // prioritization from this method applies to within each category
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList : ArrayBuffer[K] = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left: K, right: K) => map.get(left).getOrElse(Set()).size > map.get(right).getOrElse(Set()).size
    )

    val retval : ArrayBuffer[T] = new ArrayBuffer[T](keyList.size * 2)
    var index : Int = 0
    var found: Boolean = true

    while (found){
      found = false
      for (key : K <- keyList) {
        val containerList : ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(null != containerList)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }




  /**
   * Returns a standard ThreadFactory except all threads are daemons.
   */
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }
  }

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor = {
    var threadPool = Executors.newCachedThreadPool.asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Return the string to tell how long has passed in seconds. The passing parameter should be in
   * millisecond.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    return " " + (System.currentTimeMillis - startTimeMs) + " ms "
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor = {
    var threadPool = Executors.newFixedThreadPool(nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory(newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes.
   * This is used to figure out how much memory to claim from Mesos based on the SPARK_MEM
   * environment variable.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }

  /**
   * Convert a memory quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def memoryBytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Convert a memory quantity in megabytes to a human-readable string such as "4.0 MB".
   */
  def memoryMegabytesToString(megabytes: Long): String = {
    memoryBytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command in the given working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String], workingDir: File) {
    val process = new ProcessBuilder(command: _*)
        .directory(workingDir)
        .redirectErrorStream(true)
        .start()
    new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
  }

  /**
   * Execute a command in the current working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String]) {
    execute(command, new File("."))
  }


  /**
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   */
  def getSparkCallSite: String = {
    val trace = Thread.currentThread.getStackTrace().filter( el =>
      (!el.getMethodName.contains("getStackTrace")))

    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var finished = false

    for (el <- trace) {
      if (!finished) {
        if (el.getClassName.startsWith("spark.") && !el.getClassName.startsWith("spark.examples.")) {
          lastSparkMethod = if (el.getMethodName == "<init>") {
            // Spark method is a constructor; get its class name
            el.getClassName.substring(el.getClassName.lastIndexOf('.') + 1)
          } else {
            el.getMethodName
          }
        }
        else {
          firstUserLine = el.getLineNumber
          firstUserFile = el.getFileName
          finished = true
        }
      }
    }
    "%s at %s:%s".format(lastSparkMethod, firstUserFile, firstUserLine)
  }
}
