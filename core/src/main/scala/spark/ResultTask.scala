package spark

class ResultTask[T, U](
    runId: Int,
    stageId: Int,
    val rdd: RDD[T],
    val func: (TaskContext, Iterator[T]) => U,
    val partition: Int,
    locs: Seq[String],
    val outputId: Int)
  extends DAGTask[U](runId, stageId) {
  
  val split = rdd.splits(partition)

  override def input: Iterator[T] = rdd.iterator(split)

  override def run(attemptId: Int, input: Iterator[_]): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    func(context, input.asInstanceOf[Iterator[T]])
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}
