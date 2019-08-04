package com.dfossouo.scala.k2hdfs

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile



object ConsumerStreaming{

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

  def writeStreamer(input: DataFrame, checkPointFolder: String, output: String): StreamingQuery = {
    input
      .writeStream
      .format("orc")
      .option("checkpointLocation", checkPointFolder)
      .option("path", output)
      .outputMode(OutputMode.Append)
      .start()
  }

  def main(args: Array[String]) {


    // Init properties
    val props = getProps(args(0))



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> props.get("bootstrap.servers").get,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> props.get("group.id").get,
      "auto.offset.reset" -> props.get("auto.offset.reset").get,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf().setMaster("local[2]").setAppName("TestKafkaConsumer")

    lazy val streamingContext = new StreamingContext(conf, Seconds(3))


    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val ds = stream.map(record => (record.key, record.value))

    ds.foreachRDD { rdd =>

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      if (!rdd.isEmpty()) {


        println("***** here is the contain of the RDD : " + rdd)
        rdd.saveAsTextFile("/tmp/kafka_20190802/")

        writeStreamer(rdd.toDF(),"/tmp/kafka/checkpoint","/tmp/kafka/output")


      }


      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
    }


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}