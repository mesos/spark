package spark

import java.io._
import scala.util.MurmurHash

/**
 * Wraps the given OutputStream, checksumming everything that gets written to it.
 */
class ChecksummingOutputStream(os: OutputStream) extends OutputStream {
  val checksum = new MurmurHash[Byte](42) // constant seed so checksum is reproducible

  override def write(b: Int) {
    // Assume that b is really a Byte. This is part of the contract of
    // OutputStream.
    checksum(b.toByte)
    os.write(b)
  }

  override def write(b: Array[Byte]) {
    for (byte <- b) checksum(byte)
    os.write(b)
  }

  override def flush() {
    os.flush()
  }

  override def close() {
    os.close()
  }
}
