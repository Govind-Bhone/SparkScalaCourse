package com.sundogsoftware.spark
import org.apache.log4j.{Level, Logger}

import java.util.UUID
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

case class Record(aid: Long, productId: Long, ts: Long, orderId: String)

case class Book(book_name: String, cost: Int, writer_id: Int)

case class Writer(writer_name: String, writer_id: Int)
object TestApp {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("InconsistencyDetectionStreamingJob1")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(conf = conf)
      .getOrCreate


    val sc = spark.sparkContext

    import spark.implicits._

    val records = List(
      Record(1000, 100, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1001, 100, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1002, 100, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1003, 100, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1001, 200, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1004, 200, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1005, 200, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1006, 100, System.currentTimeMillis(), UUID.randomUUID().toString),
      Record(1002, 200, System.currentTimeMillis(), UUID.randomUUID().toString)

    )

    val metaMap = scala.collection.mutable.Map[Long, List[Long]](
      (1L -> List(1000L, 1001L)),
      (2L -> List(1003L, 1001L)),
      (3L -> List(1002L, 1006L))
    )

    // record data frame
    /**
     * +----+---------+-------------+--------------------+
     * | aid|productId|           ts|             orderId|
     * +----+---------+-------------+--------------------+
     * |1000|      100|1674128580179|edf9929a-f253-487...|
     * |1001|      100|1674128580179|cc41a026-63df-410...|
     * |1002|      100|1674128580179|9732755b-1207-471...|
     * |1003|      100|1674128580179|51125ddd-4129-48a...|
     * |1001|      200|1674128580179|f4917676-b08d-41e...|
     * |1004|      200|1674128580179|dc80559d-16e6-4fa...|
     * |1005|      200|1674128580179|c9b743eb-457b-455...|
     * |1006|      100|1674128580179|e8611141-3e0e-4d5...|
     * |1002|      200|1674128580179|30be34c7-394c-43a...|
     * +----+---------+-------------+--------------------+
     */
    val dataDf = sc.parallelize(records).toDF
    dataDf.show(10)

    // metadata data frame
    /**
     * +-----------+---------------+-----------+
     * |metaCompany|metaAccountList|metaAccount|
     * +-----------+---------------+-----------+
     * |          2|   [1003, 1001]|       1003|
     * |          2|   [1003, 1001]|       1001|
     * |          1|   [1000, 1001]|       1000|
     * |          1|   [1000, 1001]|       1001|
     * |          3|   [1002, 1006]|       1002|
     * |          3|   [1002, 1006]|       1006|
     * +-----------+---------------+-----------+
     */
    val metaDf = sc.parallelize(metaMap.toList)
      .toDF("metaCompany", "metaAccountList")
      .withColumn("metaAccount", explode($"metaAccountList"))
    metaDf.show(10)


    /**
     * +----+---------+-------------+--------------------+-----------+---------------+-----------+
     * | aid|productId|           ts|             orderId|metaCompany|metaAccountList|metaAccount|
     * +----+---------+-------------+--------------------+-----------+---------------+-----------+
     * |1002|      100|1674130203306|555f0596-b9fa-45d...|          3|   [1002, 1006]|       1002|
     * |1002|      200|1674130203306|31304ab7-b011-406...|          3|   [1002, 1006]|       1002|
     * |1000|      100|1674130203306|a582a177-1ff4-43b...|          1|   [1000, 1001]|       1000|
     * |1001|      100|1674130203306|101a07d4-361a-481...|          2|   [1003, 1001]|       1001|
     * |1001|      100|1674130203306|101a07d4-361a-481...|          1|   [1000, 1001]|       1001|
     * |1001|      200|1674130203306|2758e2f0-6c6e-4d5...|          2|   [1003, 1001]|       1001|
     * |1001|      200|1674130203306|2758e2f0-6c6e-4d5...|          1|   [1000, 1001]|       1001|
     * |1006|      100|1674130203306|385aa714-0bdf-4fa...|          3|   [1002, 1006]|       1006|
     * |1003|      100|1674130203306|e0c7433d-c02f-43d...|          2|   [1003, 1001]|       1003|
     * +----+---------+-------------+--------------------+-----------+---------------+-----------+
     */
/*    dataDf
      .join(metaDf, dataDf("aid") === metaDf("metaAccount"))
      .select($"aid",$"productId",$"ts",$"orderId",$"metaCompany".as("companyId"))
      .repartition($"companyId",$"productId")
      .orderBy($"ts".asc)
      .write
      .parquet("/tmp")*/


    val books = Seq(
      Book("Scala", 400, 1),
      Book("Spark", 500, 2),
      Book("Kafka", 300, 3),
      Book("Java", 350, 5)
    )

    val bookDS = sc.parallelize(books).toDS()
    bookDS.show()

    val writers = Seq(
      Writer("Martin", 1),
      Writer("Zaharia ",2),
      Writer("Neha", 3),
      Writer("James", 4)
    )
    val writerDS = sc.parallelize(writers).toDS()

    val BookWriterInner = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "inner")
    BookWriterInner.show()


    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val BookWriterCross = bookDS.join(writerDS)
    BookWriterCross.show()


    val BookWriterLeft = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftouter")
    BookWriterLeft.show()


    val BookWriterRight = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "rightouter")
    BookWriterRight.show()


    val BookWriterFull = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "fullouter")
    BookWriterFull.show()


    val BookWriterLeftSemi = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftsemi")
    BookWriterLeftSemi.show()

    val BookWriterLeftAnti = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftanti")
    BookWriterLeftAnti.show()

    val data1 = sc.parallelize(Seq(("A",1),("B",2),("C",3)))

    val data2 = sc.parallelize(Seq(("B",4),("E",5)))


    val cogroupfunc = data1.cogroup(data2)

    println(cogroupfunc.collect().toList)



    val d1 = sc.parallelize(records).toDF
    val d2 = sc.parallelize(metaMap.toList)
      .toDF("metaCompany", "metaAccountList")





  }
}