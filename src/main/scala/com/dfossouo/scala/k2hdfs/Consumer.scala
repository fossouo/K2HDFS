package com.dfossouo.scala.k2hdfs

import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object Consumer extends App {

  import java.util.Properties

  val TOPIC="test"

  val  props = new Properties()
  props.put("bootstrap.servers", "c125-node4.squadron-labs.com:6667")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true){
    val records=consumer.poll(100)
    for (record<-records.asScala){
      println(record)
    }
  }
}