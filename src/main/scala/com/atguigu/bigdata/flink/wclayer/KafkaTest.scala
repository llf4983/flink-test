package com.atguigu.bigdata.flink.wclayer

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pro = new Properties()
    pro.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    pro.setProperty("group.id","flinkGroup")
    pro.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("auto.offset.reset","latest")
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer011[String]("topic_flink",new SimpleStringSchema(),pro))
    kafkaDataStream.print()

    env.execute()
  }
}
