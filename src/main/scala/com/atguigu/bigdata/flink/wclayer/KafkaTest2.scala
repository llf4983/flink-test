package com.atguigu.bigdata.flink.wclayer

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserialozer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id","flinkGro")
    //    props.setProperty("auto.offset.reset","latest")
    //    props.setProperty("auto.offset.reset","latest")
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer011[String]("topic_flink",new SimpleStringSchema(),props))
    kafkaDataStream.print()
    env.execute()
  }

}
