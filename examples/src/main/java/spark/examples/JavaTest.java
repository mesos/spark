package spark.examples;

import spark.*;

class JavaTest {
  public static void main(String[] args) {
    System.out.println("Hello world\n");

    SparkContext ctx = new SparkContext("local", "JavaTest");
    RDD<String> rdd = ctx.textFile("LICENSE", 1);
    System.out.println(rdd.count());
  }
}
