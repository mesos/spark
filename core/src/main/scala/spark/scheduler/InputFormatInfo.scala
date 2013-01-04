package spark

import scala.collection.immutable.Set
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._


// information about a specific split instance : handles both split instances.
// So that we do not need to worry about the differences.
class SplitInfo(val inputFormatClazz: Class[_], val hostLocation: String, val path: String, 
                val length: Long, val underlyingSplit: Any) {
  override def toString(): String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + 
      ", hostLocation : " + hostLocation + ", path : " + path + 
      ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    // ignore overflow ? It is hashcode anyway !
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    hashCode
  }

  // This is practically useless since most of the Split impl's dont seem to implement equals :-(
  // So unless there is identity equality between underlyingSplits, it will always fail even if it 
  // is pointing to same block.
  override def equals(other: Any): Boolean = other match {
    case that: SplitInfo => {
      this.hostLocation == that.hostLocation && 
        this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path &&
        this.length == that.length &&
        // other split specific checks (like start for FileSplit)
        this.underlyingSplit == that.underlyingSplit
    }
    case _ => false
  }
}

object SplitInfo {

  def toSplitInfo(inputFormatClazz: Class[_], path: String, 
                  mapredSplit: org.apache.hadoop.mapred.InputSplit): Seq[SplitInfo] = {
    val retval = ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    retval
  }

  def toSplitInfo(inputFormatClazz: Class[_], path: String, 
                  mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit): Seq[SplitInfo] = {
    val retval = ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    retval
  }
}


/**
 * Parses and holds information about inputFormat (and files) specified as a parameter.
 */
class InputFormatInfo(val configuration: Configuration, val inputFormatClazz: Class[_], 
                      val path: String) extends Logging {

  var mapreduceInputFormat: Boolean = false
  var mapredInputFormat: Boolean = false

  validate()

  override def toString(): String = {
    "InputFormatInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + ", path : " + path
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + path.hashCode
    hashCode
  }

  // Since we are not doing canonicalization of path, this can be wrong : like relative vs absolute path
  // .. which is fine, this is best case effort to remove duplicates - right ?
  override def equals(other: Any): Boolean = other match {
    case that: InputFormatInfo => {
      // not checking config - that should be fine, right ?
      this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path
    }
    case _ => false
  }

  private def validate() {
    logDebug("validate InputFormatInfo : " + inputFormatClazz + ", path  " + path)

    try {
      if (classOf[org.apache.hadoop.mapreduce.InputFormat[_, _]].isAssignableFrom(inputFormatClazz)) {
        logDebug("inputformat is from mapreduce package")
        mapreduceInputFormat = true
      }
      else if (classOf[org.apache.hadoop.mapred.InputFormat[_, _]].isAssignableFrom(inputFormatClazz)) {
        logDebug("inputformat is from mapred package")
        mapredInputFormat = true
      }
      else {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " is NOT a supported input format ? does not implement either of the supported hadoop api's")
      }
    }
    catch {
      case e: ClassNotFoundException => {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz + " cannot be found ?", e)
      }
    }
  }


  // This method does not expect failures, since validate has already passed ...
  private def prefLocsFromMapreduceInputFormat(): Set[SplitInfo] = {
    val conf = new JobConf(configuration)
    FileInputFormat.setInputPaths(conf, path)

    val instance: org.apache.hadoop.mapreduce.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], conf).asInstanceOf[
        org.apache.hadoop.mapreduce.InputFormat[_, _]]
    val job = new Job(conf)

    val retval = ArrayBuffer[SplitInfo]()
    val list = instance.getSplits(job)
    for (split <- list) {
      retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, split)
    }

    return retval.toSet
  }

  // This method does not expect failures, since validate has already passed ...
  private def prefLocsFromMapredInputFormat(): Set[SplitInfo] = {
    val jobConf = new JobConf(configuration)
    FileInputFormat.setInputPaths(jobConf, path)

    val instance: org.apache.hadoop.mapred.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], jobConf).asInstanceOf[
        org.apache.hadoop.mapred.InputFormat[_, _]]

    val retval = ArrayBuffer[SplitInfo]()
    instance.getSplits(jobConf, jobConf.getNumMapTasks()).foreach(
        elem => retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, elem)
    )

    return retval.toSet
   }

  private def findPreferredLocations(): Set[SplitInfo] = {
    logDebug("mapreduceInputFormat : " + mapreduceInputFormat + ", mapredInputFormat : " + mapredInputFormat + 
      ", inputFormatClazz : " + inputFormatClazz)
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
    Typical use of this method for allocation would follow some algo like this 
    (which is what we currently do in YARN branch) :
    a) For each host, count number of splits hosted on that host.
    b) Decrement the currently allocated containers on that host.
    c) Compute rack info for each host and update rack -> count map based on (b).
    d) Allocate nodes based on (c)
    e) On the allocation result, ensure that we dont allocate "too many" jobs on a single node 
       (even if data locality on that is very high) : this is to prevent fragility of job if a single 
       (or small set of) hosts go down.

    go to (a) until required nodes are allocated.

    If a node 'dies', follow same procedure.

    PS: I know the wording here is weird, hopefully it makes some sense !
  */
  def computePreferredLocations(formats: Seq[InputFormatInfo]): HashMap[String, HashSet[SplitInfo]] = {

    val nodeToSplit = new HashMap[String, HashSet[SplitInfo]]
    for (inputSplit <- formats) {
      val splits = inputSplit.findPreferredLocations()

      for (split <- splits){
        val location = split.hostLocation
        val set = nodeToSplit.getOrElseUpdate(location, new HashSet[SplitInfo])
        set += split
      }
    }

    nodeToSplit
  }
}
