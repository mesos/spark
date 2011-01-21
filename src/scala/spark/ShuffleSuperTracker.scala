package spark

import java.io._
import java.net._
import java.util.UUID

import scala.collection.mutable.{ListBuffer, Map}

object ShuffleSuperTracker {
  // Messages
  val REGISTER_SHUFFLE_TRACKER = 0
  val UNREGISTER_SHUFFLE_TRACKER = 1
  val FIND_SHUFFLE_TRACKER = 2
  val GET_UPDATED_SHARE = 3

  // Map to keep track of different shuffle tracker's addresses
  private var uuidToTrackerMap = Map[UUID, SplitInfo] ()
  private var listOfShuffles = new ListBuffer[UUID] ()

  class SuperTracker(masterTrackerPort: Int, maxRxConnections: Int)
  extends Thread with Logging {
    override def run: Unit = {
      var threadPool = Shuffle.newDaemonCachedThreadPool
      var serverSocket: ServerSocket = null
      
      serverSocket = new ServerSocket(masterTrackerPort)
      logInfo ("ShuffleSuperTracker started: " + serverSocket)
      
      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            // TODO: No timeout for now
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => {
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute (new Thread {
                override def run: Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush
                  val ois = new ObjectInputStream(clientSocket.getInputStream)
                  
                  try {
                    // First, read message type
                    val messageType = ois.readObject.asInstanceOf[Int]
                    
                    if (messageType == REGISTER_SHUFFLE_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      // Receive tracker's hostAddress and listenPort
                      val tInfo = ois.readObject.asInstanceOf[SplitInfo]
                      
                      // Add to the map
                      uuidToTrackerMap.synchronized {
                        uuidToTrackerMap += (uuid -> tInfo)                        
                      }
                      
                      // Add to listOfShuffles
                      listOfShuffles.synchronized {
                        listOfShuffles += uuid
                      }
                      
                      logInfo ("New shuffle registered with the ShuffleSuperTracker " + uuid + " " + uuidToTrackerMap + " " + listOfShuffles)
                      
                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    } 
                    else if (messageType == UNREGISTER_SHUFFLE_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      
                      // Remove from the map
                      uuidToTrackerMap.synchronized {
                        uuidToTrackerMap -= uuid
                        // uuidToTrackerMap(uuid) = SplitInfo("", SplitInfo.ShuffleAlreadyFinished, SplitInfo.UnusedParam)
                      } 

                      // Remove from listOfShuffles
                      listOfShuffles.synchronized {
                        listOfShuffles -= uuid
                      }

                      logInfo ("Shuffle unregistered from the ShuffleSuperTracker " + uuid + " " + uuidToTrackerMap + " " + listOfShuffles)

                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    }
                    else if (messageType == FIND_SHUFFLE_TRACKER) {
                      // Receive uuid
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      
                      var tInfo: SplitInfo = null                      
                      uuidToTrackerMap.synchronized {
                        tInfo = if (uuidToTrackerMap.contains(uuid)) {
                                  uuidToTrackerMap(uuid)
                                } else {
                                  SplitInfo("", SplitInfo.TrackerDoesNotExist,
                                    SplitInfo.UnusedParam)
                                }
                      }
                        
                      logInfo ("ShuffleSuperTracker: Got new request: " + clientSocket + " for " + uuid + " : " + tInfo.listenPort)
                      
                      // Send reply back
                      oos.writeObject(tInfo)
                      oos.flush()
                    }
                    else if (messageType == GET_UPDATED_SHARE) {
                      // This is the most important part. Trackers for different
                      // shuffle get to know their share from here
                      
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      
                      // If uuid was the 1st one to start, give it full share.
                      // Basically we are doing FIFO                      
                      var allocatedConnections = -1
                      listOfShuffles.synchronized {
                        allocatedConnections = 
                          if (listOfShuffles.size > 0 && 
                              uuid.compareTo(listOfShuffles(0)) == 0) {
                           maxRxConnections
                          } else {
                            0
                          }                    
                      }
                      
                      logInfo("allocatedConnections = " + allocatedConnections)

                      oos.writeObject(allocatedConnections)
                      oos.flush()
                    }
                    else {
                      throw new SparkException("Undefined messageType at ShuffleSuperTracker")
                    }
                  } catch {
                    case e: Exception => {
                      logInfo ("ShuffleSuperTracker had a " + e)
                    }
                  } finally {
                    ois.close
                    oos.close
                    clientSocket.close
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => {
                clientSocket.close
              }
            }
          }
        }
      } finally {
        serverSocket.close
      }
      // Shutdown the thread pool
      threadPool.shutdown
    }
  }
  
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: ShuffleSuperTracker <maxRxConnections>")
      System.exit(1)
    }
    
    var masterTrackerPort = System.getProperty(
      "spark.shuffle.masterTrackerPort", "22222").toInt

    var maxRxConnections = 
      if (args.length > 0) args(0).toInt else System.getProperty(
      "spark.shuffle.maxRxConnections", "2").toInt
    
    var shuffleSuperTracker = 
      new SuperTracker(masterTrackerPort, maxRxConnections)
    shuffleSuperTracker.start()
  }
}
