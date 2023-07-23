package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("spark://192.168.1.7:7077", "HelloWorld")

    val lines = sc.textFile("hdfs:///data/ml-100k/u.data")
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    sc.stop()
  }
}
