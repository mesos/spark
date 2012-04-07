package spark

abstract class ShuffleFetcher {
  // Fetch the shuffle outputs for a given ShuffleDependency, calling func exactly
  // once on each key-value pair obtained.
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit)

  /** Deterministically computes a seed for randomizing the order of shuffle fetching. */
  protected def seed(shuffleId: Int, reduceId: Int): Long =
    (shuffleId.toLong << 32) + reduceId.toLong

  // Stop the fetcher
  def stop() {}
}
