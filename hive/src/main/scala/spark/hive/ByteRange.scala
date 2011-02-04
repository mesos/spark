package spark.hive

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.Text

import org.apache.hadoop.hive.serde2.`lazy`.LazyInteger

/**
 * A ByteRange represents a sequence of bytes in a byte array. This class
 * is used to efficiently refer to sub-arrays without copying data.
 */
class ByteRange(val bytes: Array[Byte], val start: Int, val end: Int) {
  def this(bytes: Array[Byte], end: Int) = this(bytes, 0, end)
  
  def this(bytes: Array[Byte]) = this(bytes, 0, bytes.length)

  def this() = this(Array(), 0, 0)
  
  def length: Int = end - start

  def split(separator: Byte): Seq[ByteRange] = {
    var prevPos = start
    var pos = start
    val result = new ArrayBuffer[ByteRange]()
    while (pos < end) {
      if (bytes(pos) == separator) {
        result += new ByteRange(bytes, prevPos, pos)
        prevPos = pos + 1
      }
      pos += 1
    }
    if (prevPos < end) {
      result += new ByteRange(bytes, prevPos, pos)
    }
    return result
  }

  def decodeString: String = Text.decode(bytes, start, length)

  def decodeInt: Int = LazyInteger.parseInt(bytes, start, length)

  def decodeLong: Long = java.lang.Long.parseLong(decodeString)

  def decodeDouble: Double = java.lang.Double.parseDouble(decodeString)

  def encodesNull: Boolean = {
    length == 2 && bytes(start) == '\\' && bytes(start + 1) == 'N'
  }
}

// Pattern matching object for extracting a String from a Hive field
object StringField {
  def unapply(bytes: ByteRange): Option[String] = {
    if (bytes == null || bytes.encodesNull)
      None
    else
      Some(bytes.decodeString)
  }
}

// Pattern matching object for extracting an Int from a Hive field
object IntField {
  def unapply(bytes: ByteRange): Option[Int] = {
    if (bytes == null || bytes.encodesNull)
      None
    else
      Some(bytes.decodeInt)
  }
}

// Pattern matching object for extracting a Long from a Hive field
object LongField {
  def unapply(bytes: ByteRange): Option[Long] = {
    if (bytes == null || bytes.encodesNull)
      None
    else
      Some(bytes.decodeLong)
  }
}

// Pattern matching object for extracting a Double from a Hive field
object DoubleField {
  def unapply(bytes: ByteRange): Option[Double] = {
    if (bytes == null || bytes.encodesNull)
      None
    else
      Some(bytes.decodeDouble)
  }
}








