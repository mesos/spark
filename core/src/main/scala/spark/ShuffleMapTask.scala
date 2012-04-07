package spark

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.{HashMap => JHashMap}

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
      val fbos = new FastBufferedOutputStream(new FileOutputStream(file))
      val cos =
        if (SparkEnv.get.eventReporter.enableChecksumming) Some(new ChecksummingOutputStream(fbos))
        else None
      val out = cos match {
        case Some(c) => ser.outputStream(c)
        case None => ser.outputStream(fbos)
      }
      val iter = buckets(i).entrySet().iterator()

      while (iter.hasNext()) {
        val entry = iter.next()
        out.writeObject((entry.getKey, entry.getValue))
      }

      // TODO: have some kind of EOF marker
      out.close()

      for (c <- cos) {
        SparkEnv.get.eventReporter.reportShuffleChecksum(rdd, partition, i, c.checksum.hash)
      }
    }
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
