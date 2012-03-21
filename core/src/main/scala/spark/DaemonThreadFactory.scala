package spark

import akka.dispatch.ExecutorBasedEventDrivenDispatcher
import akka.dispatch.MonitorableThreadFactory
import java.util.concurrent.ThreadFactory

/**
 * A ThreadFactory that creates daemon threads
 */
private object DaemonThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r);
    t.setDaemon(true)
    return t
  }
}

/**
 * Akka dispatcher that creates actors with daemon threads.
 */
class DaemonDispatcher(name: String) extends {
  override val threadFactory = new MonitorableThreadFactory(name) {
    override def newThread(runnable: Runnable) = {
      val thread = super.newThread(runnable)
      thread.setDaemon(true)
      thread
    }
  }
} with ExecutorBasedEventDrivenDispatcher(name)
