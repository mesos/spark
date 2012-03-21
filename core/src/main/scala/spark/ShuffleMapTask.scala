package spark

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.{HashMap => JHashMap}
import scala.util.MurmurHash

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

class ShuffleMapTask(
    runId: Int,
    stageId: Int,
    val rdd: RDD[_],
    dep: ShuffleDependency[_,_,_],
    val partition: Int, 
    locs: Seq[String])
  extends DAGTask[String](runId, stageId)
  with Logging {
  
  val split = rdd.splits(partition)

  override def input: Iterator[_] = rdd.iterator(split)

  override def run(attemptId: Int, input: Iterator[_]): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])
    for (elem <- input) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      var existing = bucket.get(k)
      if (existing == null) {
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    for (i <- 0 until numOutputSplits) {
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      val out = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
      val iter = buckets(i).entrySet().iterator()

      if (SparkEnv.get.eventReporter.enableChecksumming) {
        val checksum = new MurmurHash[(Any, Any)](42) // constant seed so checksum is reproducible
        while (iter.hasNext()) {
          val entry = iter.next()
          val pair = (entry.getKey, entry.getValue)
          out.writeObject(pair)
          checksum(pair)
        }
        SparkEnv.get.eventReporter.reportShuffleChecksum(rdd, dep.shuffleId, partition, i, checksum.hash)
      } else {
        while (iter.hasNext()) {
          val entry = iter.next()
          out.writeObject((entry.getKey, entry.getValue))
        }
      }
      // TODO: have some kind of EOF marker
      out.close()
    }
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
