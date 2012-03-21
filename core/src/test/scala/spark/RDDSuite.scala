package spark

import scala.collection.mutable.HashMap
import org.scalatest.FunSuite
import spark.SparkContext._

class RDDSuite extends FunSuite {
  test("basic operations") {
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.reduce(_ + _) === 10)
    assert(nums.fold(0)(_ + _) === 10)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    assert(partitionSums.collect().toList === List(3, 7))
    sc.stop()
  }

  test("aggregate") {
    val sc = new SparkContext("local", "test")
    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]
    val emptyMap = new StringMap {
      override def default(key: String): Int = 0
    }
    val mergeElement: (StringMap, (String, Int)) => StringMap = (map, pair) => {
      map(pair._1) += pair._2
      map
    }
    val mergeMaps: (StringMap, StringMap) => StringMap = (map1, map2) => {
      for ((key, value) <- map2) {
        map1(key) += value
      }
      map1
    }
    val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
    assert(result.toSet === Set(("a", 6), ("b", 2), ("c", 5)))
    sc.stop()
  }

  test("mapDependencies") {
    // Make an RDD graph:
    // nums <- doubled <- doubledAgain
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3))
    val doubled = nums.map(_ * 2)
    var doubledAgain = doubled.map(_ * 2)

    // Sanity check
    assert(doubledAgain.collect.toList === List(4, 8, 12))

    // Insert a transformation between doubled and doubledAgain:
    // nums <- doubled <- *new RDD* <- doubledAgain
    doubledAgain = doubledAgain.mapDependencies(new (RDD ~> RDD) {
      def apply[T](dependency: RDD[T]): RDD[T] = {
        doubled.map(_ + 1).asInstanceOf[RDD[T]]
      }
    })

    // Check that it worked
    assert(doubledAgain.collect.toList === List(6, 10, 14))
  }
}
