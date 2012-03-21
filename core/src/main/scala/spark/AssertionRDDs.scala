package spark

/**
 * Passes each element of prev through unchanged, but also runs elementAssertion on each element and
 * reports assertion failures.
 */
class ElementAssertionRDD[T: ClassManifest](
  prev: RDD[T],
  elementAssertion: (T, Split) => Option[AssertionFailure]
) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = {
    prev.iterator(split).map { (t: T) =>
      for (failure <- elementAssertion(t, split)) {
        SparkEnv.get.eventReporter.reportAssertionFailure(failure)
      }
      t
    }
  }
  override def mapDependencies(g: RDD ~> RDD) = new ElementAssertionRDD(g(prev), elementAssertion)

  reportCreation()
}

/**
 * Passes each element of prev through unchanged, but also reduces the elements in each partition
 * independently and reports assertion failures on the result.
 */
class ReduceAssertionRDD[T: ClassManifest](
  prev: RDD[T],
  reducer: (T, T) => T,
  assertion: (T, Split) => Option[AssertionFailure]
) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = new ReducingIterator(prev.iterator(split), split)
  override def mapDependencies(g: RDD ~> RDD) = new ReduceAssertionRDD(g(prev), reducer, assertion)

  class ReducingIterator(underlying: Iterator[T], split: Split) extends Iterator[T] {
    def hasNext = underlying.hasNext
    def next(): T =
      if (hasNext) {
        val result = underlying.next()
        updateIntermediate(result)
        if (!hasNext) checkAssertion()
        result
      } else {
        throw new UnsupportedOperationException("no more elements")
      }
    private var intermediate: Option[T] = None
    private def updateIntermediate(elem: T) {
      intermediate = intermediate match {
        case None => Some(elem)
        case Some(x) => Some(reducer(x, elem))
      }
    }
    private def checkAssertion() {
      for (x <- intermediate; failure <- assertion(x, split)) {
        SparkEnv.get.eventReporter.reportAssertionFailure(failure)
      }
    }
  }

  reportCreation()
}
