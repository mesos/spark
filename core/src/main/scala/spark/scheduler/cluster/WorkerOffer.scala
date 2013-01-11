package spark.scheduler.cluster

/**
 * Represents free resources available on a worker node.
 */
private[spark]
class WorkerOffer(val slaveId: String, val hostPort: String, val cores: Int) {
}
