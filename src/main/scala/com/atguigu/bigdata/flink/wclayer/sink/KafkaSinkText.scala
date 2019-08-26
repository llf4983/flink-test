package com.atguigu.bigdata.flink.wclayer.sink

import java.util.Random

import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkText {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sensorDS = env.addSource(new MyOwnSoource())

    sensorDS.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","topic_flink",new SimpleStringSchema()))

    env.execute()
  }
}

class MyOwnSoource() extends RichSourceFunction[String]{
  var flag=true



  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def cancel(): Unit = {flag=false}

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random()
    while (flag){
      sourceContext.collect(random.nextInt()+"")
      Thread.sleep(500)
    }
  }
}
