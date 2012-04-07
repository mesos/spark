package spark

import java.io._
import scala.collection.mutable
import scala.collection.immutable

/**
 * Writes events to an event log on disk and verifies RDD checksums.
 */
class EventLogWriter extends Logging {
  private var eventLog: Option[EventLogOutputStream] = None
  setEventLogPath(Option(System.getProperty("spark.arthur.logPath")))
  private var eventLogReader: Option[EventLogReader] = None
  val checksums = new mutable.HashMap[Any, immutable.HashSet[ChecksumEvent]]
  val checksumMismatches = new mutable.ArrayBuffer[ChecksumEvent]

  def setEventLogPath(eventLogPath: Option[String]) {
    eventLog =
      for {
        elp <- eventLogPath
        file = new File(elp)
        if !file.exists
      } yield new EventLogOutputStream(new FileOutputStream(file))
  }

  def log(entry: EventLogEntry) {
    for (l <- eventLog) {
      l.writeObject(entry)
    }

    for (r <- eventLogReader) {
      r.addEvent(entry)
    }

    entry match {
      case c: ChecksumEvent => processChecksumEvent(c)
      case _ => {}
    }
  }

  def flush() {
    for (l <- eventLog) {
      l.flush()
    }
  }

  def stop() {
    for (l <- eventLog) {
      l.close()
    }
  }

  private[spark] def registerEventLogReader(r: EventLogReader) {
    eventLogReader = Some(r)
  }

  private[spark] def processChecksumEvent(c: ChecksumEvent) {
    if (checksums.contains(c.key)) {
      if (!checksums(c.key).contains(c)) {
        if (checksums(c.key).exists(_.mismatch(c))) reportChecksumMismatch(c)
        checksums(c.key) += c
      }
    } else {
      checksums.put(c.key, immutable.HashSet(c))
    }
  }

  private def reportChecksumMismatch(c: ChecksumEvent) {
    checksumMismatches += c
    logWarning(c.warningString)
  }
}
