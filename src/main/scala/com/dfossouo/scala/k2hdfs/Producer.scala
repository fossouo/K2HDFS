package com.dfossouo.scala.k2hdfs

object Producer extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "c225-node2.squadron-labs.com:6667,c225-node3.squadron-labs.com:6667,c225-node4.squadron-labs.com:6667")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test"

  for(i<- 1 to 50){
    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()
}