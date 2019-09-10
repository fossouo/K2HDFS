package com.dfossouo.scala.k2hdfs

import com.dfossouo.scala.k2hdfs.ConsumerStreaming.getProps

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

object Producer extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

  val props = getProps(args(0))

  val bootstrap_servers = props.get("bootstrap.servers").get
  val key_serializer = props.get("key.serializer").get
  val value_serializer = props.get("value.serializer").get

  val topics = props.get("topic.name").get

  val  props_producer = new Properties()
  props_producer.put("bootstrap.servers", bootstrap_servers)

  props_producer.put("key.serializer", key_serializer)
  props_producer.put("value.serializer", value_serializer)


  val producer = new KafkaProducer[String, String](props_producer)

  val TOPIC = topics

  for(i<- 1 to 1000000){
    val record = new ProducerRecord(TOPIC, "key", s"hi world, $i")
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()
}