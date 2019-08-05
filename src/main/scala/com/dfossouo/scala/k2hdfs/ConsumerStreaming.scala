package com.dfossouo.scala.k2hdfs

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile



object ConsumerStreaming{

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

  def main(args: Array[String]) {


    // Init properties
    val props = getProps(args(0))


    val conf = new SparkConf().setMaster("local[2]").setAppName("TestKafkaConsumer")

//    lazy val streamingContext = new StreamingContext(conf, Seconds(10))


    val topics = Array(props.get("topic.name").get)


    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.get("bootstrap.servers").get)
      .option("subscribe", topics.toString)
      .option("startingOffsets", props.get("auto.offset.reset").get)
      .option("groupIdPrefix", props.get("group.id").get)
      .option("failOnDataLoss", props.get("failOnDataLoss").get)
      .load.select($"value".cast("string").alias("value"))


    ds1.printSchema()

    val batch = props.get("batch.duration").get.toInt

    ds1.
      writeStream.
      format("console").
      option("checkpointLocation", props.get("hdfs.checkpoint.dir").get). // <-- checkpoint directory
      trigger(Trigger.ProcessingTime(s"$batch seconds")).
      start.awaitTermination()

  }
}