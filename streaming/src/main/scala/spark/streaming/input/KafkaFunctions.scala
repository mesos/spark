package spark.streaming.input

import spark.storage.StorageLevel
import spark.streaming.StreamingContext
import spark.streaming.DStream
import spark.streaming.dstream.KafkaInputDStream

/**
 * A wrapper around StreamingContext for exposing Kafka functions. This is done in a separate
 * class so that programs that don't use Kafka don't need to link against it. In Scala, an
 * implicit conversion in StreamingContext allows one to call these functions.
 */
class KafkaFunctions(self: StreamingContext) {
  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param zkQuorum Zookeper quorum (hostname:port,hostname:port,..).
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def kafkaStream(
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[String] = {
    val kafkaParams = Map[String, String](
      "zk.connect" -> zkQuorum, "groupid" -> groupId, "zk.connectiontimeout.ms" -> "10000")
    kafkaStream[String, kafka.serializer.StringDecoder](kafkaParams, topics, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param kafkaParams Map of kafka configuration paramaters.
   *                    See: http://kafka.apache.org/configuration.html
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def kafkaStream[T: ClassManifest, D <: kafka.serializer.Decoder[_]: Manifest](
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): DStream[T] = {
    val inputStream = new KafkaInputDStream[T, D](self, kafkaParams, topics, storageLevel)
    self.registerInputStream(inputStream)
    inputStream
  }
}
