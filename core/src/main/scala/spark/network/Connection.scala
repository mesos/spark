package spark.network

import spark._

import scala.collection.mutable.{HashMap, Queue, ArrayBuffer}

import java.io._
import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.net._


private[spark]
abstract class Connection(val channel: SocketChannel, val selector: Selector) extends Logging {

  channel.configureBlocking(false)
  channel.socket.setTcpNoDelay(true)
  channel.socket.setReuseAddress(true)
  channel.socket.setKeepAlive(true)
  /*channel.socket.setReceiveBufferSize(32768) */

  var onCloseCallback: Connection => Unit = null
  var onExceptionCallback: (Connection, Exception) => Unit = null
  var onKeyInterestChangeCallback: (Connection, Int) => Unit = null

  val remoteAddress = getRemoteAddress()
  private val socketRemoteConnectionManagerId: ConnectionManagerId = ConnectionManagerId.fromSocketAddress(remoteAddress)

  def getRemoteConnectionManagerId(): ConnectionManagerId = {
    socketRemoteConnectionManagerId
  }

  def key() = channel.keyFor(selector)

  def getRemoteAddress() = channel.socket.getRemoteSocketAddress().asInstanceOf[InetSocketAddress]

  // Returns whether we have to register for further reads or not.
  def read(): Boolean = {
    throw new UnsupportedOperationException("Cannot read on connection of type " + this.getClass.toString) 
  }

  // Returns whether we have to register for further writes or not.
  def write(): Boolean = {
    throw new UnsupportedOperationException("Cannot write on connection of type " + this.getClass.toString) 
  }

  def close() {
    val k = key()
    if (k != null) {
      k.cancel()
    }
    channel.close()
    callOnCloseCallback()
  }

  def onClose(callback: Connection => Unit) {onCloseCallback = callback}

  def onException(callback: (Connection, Exception) => Unit) {onExceptionCallback = callback}

  def onKeyInterestChange(callback: (Connection, Int) => Unit) {onKeyInterestChangeCallback = callback}

  def callOnExceptionCallback(e: Exception) {
    if (onExceptionCallback != null) {
      onExceptionCallback(this, e)
    } else {
      logError("Error in connection to " + getRemoteConnectionManagerId() +
        " and OnExceptionCallback not registered", e)
    }
  }
  
  def callOnCloseCallback() {
    if (onCloseCallback != null) {
      onCloseCallback(this)
    } else {
      logWarning("Connection to " + getRemoteConnectionManagerId() +
        " closed and OnExceptionCallback not registered")
    }

  }

  def changeConnectionKeyInterest(ops: Int) {
    if (onKeyInterestChangeCallback != null) {
      onKeyInterestChangeCallback(this, ops)
    } else {
      throw new Exception("OnKeyInterestChangeCallback not registered")
    }
  }

  def printRemainingBuffer(buffer: ByteBuffer) {
    val bytes = new Array[Byte](buffer.remaining)
    val curPosition = buffer.position
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    buffer.position(curPosition)
    print(" (" + bytes.size + ")")
  }

  def printBuffer(buffer: ByteBuffer, position: Int, length: Int) {
    val bytes = new Array[Byte](length)
    val curPosition = buffer.position
    buffer.position(position)
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    print(" (" + position + ", " + length + ")")
    buffer.position(curPosition)
  }

}


private[spark] class SendingConnection(val address: InetSocketAddress, selector_ : Selector) 
extends Connection(SocketChannel.open, selector_) {

  class Outbox(fair: Int = 0) {
    val messages = new Queue[Message]()
    val defaultChunkSize = 65536  //32768 //16384 
    var nextMessageToBeUsed = 0

    def addMessage(message: Message) {
      messages.synchronized{ 
        /*messages += message*/
        messages.enqueue(message)
        logDebug("Added [" + message + "] to outbox for sending to [" + getRemoteConnectionManagerId() + "]")
      }
    }

    def getChunk(): Option[MessageChunk] = {
      fair match {
        case 0 => getChunkFIFO()
        case 1 => getChunkRR()
        case _ => throw new Exception("Unexpected fairness policy in outbox")
      }
    }

    private def getChunkFIFO(): Option[MessageChunk] = {
      /*logInfo("Using FIFO")*/
      messages.synchronized {
        while (!messages.isEmpty) {
          val message = messages(0)
          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages += message  // this is probably incorrect, it wont work as fifo
            if (!message.started) logDebug("Starting to send [" + message + "]")
            message.started = true
            return chunk 
          } else {
            /*logInfo("Finished sending [" + message + "] to [" + getRemoteConnectionManagerId() + "]")*/
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + getRemoteConnectionManagerId() +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
    
    private def getChunkRR(): Option[MessageChunk] = {
      messages.synchronized {
        while (!messages.isEmpty) {
          /*nextMessageToBeUsed = nextMessageToBeUsed % messages.size */
          /*val message = messages(nextMessageToBeUsed)*/
          val message = messages.dequeue
          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages.enqueue(message)
            nextMessageToBeUsed = nextMessageToBeUsed + 1
            if (!message.started) {
              logDebug("Starting to send [" + message + "] to [" + getRemoteConnectionManagerId() + "]")
              message.started = true
              message.startTime = System.currentTimeMillis
            }
            logTrace("Sending chunk from [" + message+ "] to [" + getRemoteConnectionManagerId() + "]")
            return chunk 
          } else {
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + getRemoteConnectionManagerId() +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
  }
  
  private val outbox = new Outbox(1)
  val currentBuffers = new ArrayBuffer[ByteBuffer]()

  /*channel.socket.setSendBufferSize(256 * 1024)*/

  override def getRemoteAddress() = address 

  def send(message: Message) {
    outbox.synchronized {
      outbox.addMessage(message)
      if (channel.isConnected) {
        changeConnectionKeyInterest(SelectionKey.OP_WRITE)
      }
    }
  }

  // MUST be called within the selector loop
  def connect() {
    try{
      channel.register(selector, SelectionKey.OP_CONNECT)
      channel.connect(address)
      logInfo("Initiating connection to [" + address + "]")
    } catch {
      case e: Exception => {
        logError("Error connecting to " + address, e)
        callOnExceptionCallback(e)
      }
    }
  }

  def finishConnect(force: Boolean): Boolean = {
    try {
      // Typically, this should finish immediately since it was triggered by a connect
      // selection - though need not necessarily always complete successfully.
      val connected = channel.finishConnect
      if (!force && !connected) {
        logInfo("finish connect failed [" + address + "], " + outbox.messages.size + " messages pending")
        return false
      }

      // Fallback to previous behavior - assume finishConnect completed
      // This will happen only when finishConnect failed for some repeated number of times (10 or so)
      // Is highly unlikely unless there was an unclean close of socket, etc
      changeConnectionKeyInterest(SelectionKey.OP_WRITE)
      logInfo("Connected to [" + address + "], " + outbox.messages.size + " messages pending")
      return true
    } catch {
      case e: Exception => {
        logWarning("Error finishing connection to " + address, e)
        callOnExceptionCallback(e)
        // ignore
        return true
      }
    }
  }

  override def write(): Boolean = {
    try{
      while(true) {
        if (currentBuffers.size == 0) {
          outbox.synchronized {
            outbox.getChunk() match {
              case Some(chunk) => {
                currentBuffers ++= chunk.buffers 
              }
              case None => {
                // changeConnectionKeyInterest(0)
                /*key.interestOps(0)*/
                return false
              }
            }
          }
        }
        
        if (currentBuffers.size > 0) {
          val buffer = currentBuffers(0)
          val remainingBytes = buffer.remaining
          val writtenBytes = channel.write(buffer)
          if (buffer.remaining == 0) {
            currentBuffers -= buffer
          }
          if (writtenBytes < remainingBytes) {
            // re-register for write.
            return true
          }
        }
      }
    } catch {
      case e: Exception => { 
        logWarning("Error writing in connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallback(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    return true
  }
}


// Must be created within selector loop - else deadlock
private[spark] class ReceivingConnection(channel_ : SocketChannel, selector_ : Selector) 
extends Connection(channel_, selector_) {
  
  class Inbox() {
    val messages = new HashMap[Int, BufferMessage]()
    
    def getChunk(header: MessageChunkHeader): Option[MessageChunk] = {
      
      def createNewMessage: BufferMessage = {
        val newMessage = Message.create(header).asInstanceOf[BufferMessage]
        newMessage.started = true
        newMessage.startTime = System.currentTimeMillis
        logDebug("Starting to receive [" + newMessage + "] from [" + getRemoteConnectionManagerId() + "]")
        messages += ((newMessage.id, newMessage))
        newMessage
      }
      
      val message = messages.getOrElseUpdate(header.id, createNewMessage)
      logTrace("Receiving chunk of [" + message + "] from [" + getRemoteConnectionManagerId() + "]")
      message.getChunkForReceiving(header.chunkSize)
    }
    
    def getMessageForChunk(chunk: MessageChunk): Option[BufferMessage] = {
      messages.get(chunk.header.id) 
    }

    def removeMessage(message: Message) {
      messages -= message.id
    }
  }

  @volatile private var inferredRemoteManagerId: ConnectionManagerId = null
  override def getRemoteConnectionManagerId(): ConnectionManagerId = {
    val currId = inferredRemoteManagerId
    if (null != currId) currId else super.getRemoteConnectionManagerId()
  }

  // The reciever's remote address is the local socket on remote side : which is NOT the connection manager id of the receiver.
  // We infer that from the messages we receive on the receiver socket.
  private def processConnectionManagerId(header: MessageChunkHeader) {
    val currId = inferredRemoteManagerId
    if (null == header.address || null != currId) return

    val managerId = ConnectionManagerId.fromSocketAddress(header.address)

    if (null != managerId) {
      inferredRemoteManagerId = managerId
    }
  }


  val inbox = new Inbox()
  val headerBuffer: ByteBuffer = ByteBuffer.allocate(MessageChunkHeader.HEADER_SIZE)
  var onReceiveCallback: (Connection , Message) => Unit = null
  var currentChunk: MessageChunk = null

  channel.register(selector, SelectionKey.OP_READ)

  override def read(): Boolean = {
    try {
      while (true) {
        if (currentChunk == null) {
          val headerBytesRead = channel.read(headerBuffer)
          if (headerBytesRead == -1) {
            close()
            return false
          }
          if (headerBuffer.remaining > 0) {
            // re-register for read event ...
            return true
          }
          headerBuffer.flip
          if (headerBuffer.remaining != MessageChunkHeader.HEADER_SIZE) {
            throw new Exception("Unexpected number of bytes (" + headerBuffer.remaining + ") in the header")
          }
          val header = MessageChunkHeader.create(headerBuffer)
          headerBuffer.clear()

          processConnectionManagerId(header)

          header.typ match {
            case Message.BUFFER_MESSAGE => {
              if (header.totalSize == 0) {
                if (onReceiveCallback != null) {
                  onReceiveCallback(this, Message.create(header))
                }
                currentChunk = null
                // re-register for read event ...
                return true
              } else {
                currentChunk = inbox.getChunk(header).orNull
              }
            }
            case _ => throw new Exception("Message of unknown type received")
          }
        }
        
        if (currentChunk == null) throw new Exception("No message chunk to receive data")
       
        val bytesRead = channel.read(currentChunk.buffer)
        if (bytesRead == 0) {
          // re-register for read event ...
          return true
        } else if (bytesRead == -1) {
          close()
          return false
        }

        /*logDebug("Read " + bytesRead + " bytes for the buffer")*/
        
        if (currentChunk.buffer.remaining == 0) {
          /*println("Filled buffer at " + System.currentTimeMillis)*/
          val bufferMessage = inbox.getMessageForChunk(currentChunk).get
          if (bufferMessage.isCompletelyReceived) {
            bufferMessage.flip
            bufferMessage.finishTime = System.currentTimeMillis
            logDebug("Finished receiving [" + bufferMessage + "] from [" + getRemoteConnectionManagerId() + "] in " + bufferMessage.timeTaken)
            if (onReceiveCallback != null) {
              onReceiveCallback(this, bufferMessage)
            }
            inbox.removeMessage(bufferMessage)
          }
          currentChunk = null
        }
      }
    } catch {
      case e: Exception  => { 
        logWarning("Error reading from connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallback(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    return true
  }
  
  def onReceive(callback: (Connection, Message) => Unit) {onReceiveCallback = callback}
}
