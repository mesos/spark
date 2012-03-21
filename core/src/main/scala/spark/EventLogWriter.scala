package spark

import java.io._

/**
 * Writes events to an event log on disk, cooperating with EventLogReader to perform checksum
 * verification if appropriate.
 */
class EventLogWriter extends Logging {
  private var eventLog: Option[EventLogOutputStream] = None
  setEventLogPath(Option(System.getProperty("spark.arthur.logPath")))
  private var eventLogReader: Option[EventLogReader] = None

  def enableChecksumVerification(eventLogReader: EventLogReader) {
    this.eventLogReader = Some(eventLogReader)
  }

  def setEventLogPath(eventLogPath: Option[String]) {
    eventLog =
      for {
        elp <- eventLogPath
        file = new File(elp)
        if !file.exists
      } yield new EventLogOutputStream(new FileOutputStream(file))
  }

  def log(entry: EventLogEntry) {
    // Log the entry
    for (l <- eventLog) {
      l.writeObject(entry)
    }

    // Do checksum verification if enabled. TODO: This takes O(n) time in the number of events; we
    // should index checksum events so it takes O(1) time instead
    for {
      r <- eventLogReader
      checksum @ (_x: ChecksumEvent) <- Some(entry)
      recordedChecksum @ (_x: ChecksumEvent) <- r.events
      if checksum mismatch recordedChecksum
    } r.reportChecksumMismatch(recordedChecksum, checksum)
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
}
