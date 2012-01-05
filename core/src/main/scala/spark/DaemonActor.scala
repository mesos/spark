package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.dispatch.ExecutorBasedEventDrivenDispatcher
import akka.dispatch.MessageDispatcher
import akka.dispatch.MonitorableThread
import akka.dispatch.MonitorableThreadFactory


/**
 * An Akka actor that uses daemon threads for its dispatcher to avoid stopping
 * the JVM when the user's main thread exits.
 */
trait DaemonActor extends Actor {
  self.dispatcher = DaemonActor.dispatcher
}

object DaemonActor {
  val dispatcher = new DaemonDispatcher("DaemonActors")
}

class DaemonDispatcher(name: String) extends {
  override val threadFactory = new MonitorableThreadFactory(name) {
    override def newThread(runnable: Runnable) = {
      val thread = super.newThread(runnable)
      thread.setDaemon(true)
      thread
    }
  }
} with ExecutorBasedEventDrivenDispatcher(name)
