package com.dfossouo.scala.k2hdfs

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.concurrent.duration._
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile



object ProducerFHDFS{

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

  def main(args: Array[String]) {


    // Init properties
    val props = getProps(args(0))

    val topics = props.get("topic.name").get
    val sourcedir = props.get("hdfs.output.dir").get


    val spark = SparkSession.builder.appName("TestKafkaConsumer")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val ds1 = spark
        .read
        .format("parquet")
        .option("compression", "snappy")
        .load(sourcedir)

    println(ds1.printSchema())

    ds1.selectExpr(topics, "CAST(Value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", props.get("bootstrap.servers").get)
      .save()

  }
}