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
