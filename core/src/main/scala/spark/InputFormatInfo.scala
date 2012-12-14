package spark

import scala.collection.immutable.Set
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}


// Host information about a split - pvt to class.
class SplitInfo(val inputFormatClazz: Class[_], val hostLocation: String, val path: String, val length: Long, val underlyingSplit: Any) {
  override def toString() : String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + ", hostLocation : " + hostLocation +
      ", path : " + path + ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode() : Int = {
    var hashCode : Int = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    // ignore overflow ?
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    hashCode
  }

  // This is practically useless since most of the Split's dont seem to implement equals :-(
  // So unless there is identity equality between underlyingSplits, it will always fail even if it is pointing to same block.
  override def equals(other: Any): Boolean = other match {
    case that: SplitInfo => {
      this.hostLocation == that.hostLocation && equalsWithHostIgnore(other.asInstanceOf[SplitInfo])
    }
    case _ => false
  }

  def equalsWithHostIgnore(that: SplitInfo) : Boolean = {
    this.inputFormatClazz == that.inputFormatClazz &&
      this.path == that.path &&
      this.length == that.length &&
      // other split specific checks (like start for FileSplit)
      this.underlyingSplit == that.underlyingSplit
  }
}

object SplitInfo {

  def toSplitInfo(inputFormatClazz: Class[_], path: String, mapredSplit: org.apache.hadoop.mapred.InputSplit) : Seq[SplitInfo] = {
    val retval = ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    retval
  }

  def toSplitInfo(inputFormatClazz: Class[_], path: String, mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit) : Seq[SplitInfo] = {
    val retval = ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    retval
  }
}


/**
  * Parses and holds information about inputFormat (and files) specified as an parameter.
  */
class InputFormatInfo(val configuration: Configuration, val inputFormatClazz: Class[_], val path: String) extends Logging {

  // Validate even before we ship job to cluster.
  var mapreduceInputFormat: Boolean = false
  var mapredInputFormat: Boolean = false
  // identify this by the input format class + path ... used to identify a block by using it in conjugation with the length (for generating a 'block-id').
  var inputIndentifier: String = null
  validate()

  override def toString() : String = {
    "InputFormatInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + ", path : " + path
  }

  override def hashCode() : Int = {
    var hashCode : Int = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + path.hashCode
    hashCode
  }

  // Since we are not doing canonicalization of path, this can be wrong ...
  override def equals(other: Any): Boolean = other match {
    case that: InputFormatInfo => {
      // not checking config - that should be fine, right ?
      this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path
    }
    case _ => false
  }

  private def validate() {
    logInfo("validate InputFormatInfo : " + inputFormatClazz + ", path  " + path)

    try {
      if (classOf[org.apache.hadoop.mapreduce.InputFormat[_, _]].isAssignableFrom(inputFormatClazz)) {
        logInfo("inputformat is form mapreduce package")
        mapreduceInputFormat = true
      }
      else if (classOf[org.apache.hadoop.mapred.InputFormat[_, _]].isAssignableFrom(inputFormatClazz)) {
        logInfo("inputformat is form mapred package")
        mapredInputFormat = true
      }
      else {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " is NOT a supported input format ? does not implement either of the supported interfaces !")
      }
    }
    catch {
      case e: ClassNotFoundException => {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz + " cannot be found ?")
      }
    }
    // '*' as delimiter since it cant be in classname or path.
    this.inputIndentifier = inputFormatClazz.getName + "*" + path
  }


  // This method does not expect failures, since validate has already passed ...
  private def prefLocsFromMapreduceInputFormat(): Set[SplitInfo] = {
    val conf : JobConf = new JobConf(configuration)
    FileInputFormat.setInputPaths(conf, path)

    val instance : org.apache.hadoop.mapreduce.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], conf).asInstanceOf[org.apache.hadoop.mapreduce.InputFormat[_, _]]
    val job : Job = new Job(conf)

    val retval = ArrayBuffer[SplitInfo]()
    val list : java.util.List[org.apache.hadoop.mapreduce.InputSplit] = instance.getSplits(job)
    import scala.collection.JavaConversions._
    for (split <- list) {
      retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, split)
    }

    return retval.toSet
  }

  private def prefLocsFromMapredInputFormat(): Set[SplitInfo] = {
    val jobConf : JobConf = new JobConf(configuration)
    FileInputFormat.setInputPaths(jobConf, path)

    val instance : org.apache.hadoop.mapred.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], jobConf).asInstanceOf[org.apache.hadoop.mapred.InputFormat[_, _]]
    // val job : JobConf = new JobConf(conf)

    val retval : ArrayBuffer[SplitInfo] = ArrayBuffer[SplitInfo]()
    instance.getSplits(// job,
      jobConf, jobConf.getNumMapTasks()).foreach(
        elem => retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, elem)
    )

    return retval.toSet
   }

  private def findPreferredLocations() : Set[SplitInfo] = {
    logInfo("mapreduceInputFormat : " + mapreduceInputFormat + ", mapredInputFormat : " + mapredInputFormat + ", inputFormatClazz : " + inputFormatClazz)
    if (mapreduceInputFormat) {
      return prefLocsFromMapreduceInputFormat()
    }
    else {
      assert(mapredInputFormat)
      return prefLocsFromMapredInputFormat()
    }
  }
}




object InputFormatInfo {
  /*
    Computes the preferred locations based on input(s) and returned a location to block map.
    Typical use of this method for allocation would follow some algo like this :
    Make copy of this data structure.
    a) Pick the preferred location with highest cardinality of set.
    b) Remove it from set - attempt allocation.
    c) Remove all blocks for this node from all other set's.
    go to (a) until required nodes are not allocated.

    If a node 'dies', follow similar procedure : except initialize from this map and remove all already allocated nodes blocks.

    PS: I know the wording here is weird, hopefully it makes some sense !
  */
  def computePreferredLocations(formats: Seq[InputFormatInfo]) : HashMap[String, HashSet[SplitInfo]] = {

    // For now, simply add up the number of times a node is present in preferred locations in various splits.
    // A single block can have the node only once (usually gauranteed by hadoop anyway).
    val nodeToSplit : HashMap[String, HashSet[SplitInfo]] = new HashMap[String, HashSet[SplitInfo]]
    for (inputSplit : InputFormatInfo <- formats) {
      val splits : Set[SplitInfo] = inputSplit.findPreferredLocations()

      println("inputSplit " + inputSplit.toString + ", splits ... " + splits.toString)

      for (split : SplitInfo <- splits){
        val location = split.hostLocation
        val set : HashSet[SplitInfo] = nodeToSplit.get(location).getOrElse(new HashSet[SplitInfo])
        set += split
        nodeToSplit.put(location, set)
      }
    }

    println("preferred locations for " + formats.toString + " is " + nodeToSplit.toString)

    nodeToSplit
  }
}
