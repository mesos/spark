package spark.hive

import spark.{SparkContext, RDD}

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text

object HiveSequenceFile {
  def create(sc: SparkContext, path: String, fields: Array[Int])
      : RDD[Array[ByteRange]] = {
    val records = sc.sequenceFile[BytesWritable, Text](path)
    val separator: Byte = 1
    return records.map { case (key, text) => 
      val result = new Array[ByteRange](fields.length)
      val parts = new ByteRange(text.getBytes, text.getLength).split(separator)
      for (i <- 0 until fields.length) {
        if (fields(i) < parts.length) {
          result(i) = parts(fields(i))
        }
      }
      result
    }
  }
}
