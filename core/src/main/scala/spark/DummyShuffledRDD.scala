package spark

/**
 * Dummy RDD used to wrap a ShuffleDependency in order to force the
 * scheduler to execute the shuffle.
 */
class DummyShuffledRDD(dep: ShuffleDependency[_,_,_]) extends RDD[Nothing](dep.rdd.context) {
  @transient
  private var splits_ = Array.tabulate[Split](dep.partitioner.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  override val dependencies = List(dep)
  override def mapDependencies(g: RDD ~> RDD) = throw new UnsupportedOperationException("not implemented")
  override def compute(split: Split): Iterator[Nothing] = Iterator.empty

  private def writeObject(stream: java.io.ObjectOutputStream) {
    stream.defaultWriteObject()
    stream match {
      case _: EventLogOutputStream =>
        stream.writeObject(splits_)
      case _ => {}
    }
  }

  private def readObject(stream: java.io.ObjectInputStream) {
    stream.defaultReadObject()
    stream match {
      case s: EventLogInputStream =>
        splits_ = s.readObject().asInstanceOf[Array[Split]]
      case _ => {}
    }
  }

  reportCreation()
}
