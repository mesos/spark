package spark.scheduler.cluster

import spark._
import spark.deploy.yarn.{ApplicationMaster, YarnAllocationHandler}
import org.apache.hadoop.conf.Configuration

/**
 *
 * This is a simple extension to ClusterScheduler - to ensure that appropriate initialization of ApplicationMaster, etc is done
 */
private[spark] class YarnClusterScheduler(sc: SparkContext, conf : Configuration) extends ClusterScheduler(sc) {

  def this(sc: SparkContext) = this(sc, new Configuration())

  // Nothing else for now ... initialize application master : which needs sparkContext to determine how to allocate
  // Note that only the first creation of SparkContext influences (and ideally, there must be only one SparkContext, right ?)
  // Subsequent creations are ignored - since nodes are already allocated by then.
  ApplicationMaster.sparkContextInitialized(sc)


  // By default, rack is unknown
  override def getRackForHost(hostPort: String) : Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    val retval : String = YarnAllocationHandler.lookupRack(conf, host)
    if (null != retval) Some(retval) else None
  }
}
