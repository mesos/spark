package spark.examples

import spark.SparkContext
import spark.SparkContext._
import java.util.Random

object ParallelGroupByTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: GroupByTest <host> [numMappers] [numKVPairs] [KeySize] [numReducers]")
      System.exit(1)
    }  
    
    var numMappers = if (args.length > 1) args(1).toInt else 2
    var numKVPairs = if (args.length > 2) args(2).toInt else 1000
    var valSize = if (args.length > 3) args(3).toInt else 1000
    var numReducers = if (args.length > 4) args(4).toInt else numMappers

    val sc = new SparkContext(args(0), "GroupBy Test")
    
    var thrd1 = new Thread {  
      override def run = {
        val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
          val ranGen = new Random
          var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
          for (i <- 0 until numKVPairs) {
            val byteArr = new Array[Byte](valSize)
            ranGen.nextBytes(byteArr)
            arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
          }
          arr1
        }.cache
        // Enforce that everything has been calculated and in cache
        pairs1.count
        
        println(pairs1.groupByKey(numReducers).count)
      }    
    }
    thrd1.start()

    var thrd2 = new Thread {  
      override def run = {
        val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
          val ranGen = new Random
          var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
          for (i <- 0 until numKVPairs) {
            val byteArr = new Array[Byte](valSize)
            ranGen.nextBytes(byteArr)
            arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
          }
          arr1
        }.cache
        // Enforce that everything has been calculated and in cache
        pairs1.count
        
        println(pairs1.groupByKey(numReducers).count)
      }    
    }
    thrd2.start()
  }
}

