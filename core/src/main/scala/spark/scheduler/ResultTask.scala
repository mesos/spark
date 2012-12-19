package spark.scheduler

import spark._

private[spark] class ResultTask[T, U](
    stageId: Int,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    val partition: Int,
    @transient locs: Seq[String],
    val outputId: Int)
  extends Task[U](stageId) {
  
  val split = rdd.splits(partition)

  // data locality is on a per host basis, not hyper specific to container (host:port). Unique on set of hosts.
  val preferredLocs : Seq[String] = if (null == locs) null else locs.map(loc => Utils.parseHostPort(loc)._1).toSet.toSeq

  {
    // DEBUG code
    preferredLocs.foreach (host => Utils.checkHost(host, "preferredLocs : " + preferredLocs))
  }

  override def run(attemptId: Long): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
