package com.atguigu.bigdata.flink.wclayer

import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.streaming.api.scala._

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.fromCollection(List(Sensor("sensor_1",1547718199, 35.80018327300259),Sensor("sensor_6", 1547718201, 15.402984393403084),Sensor("sensor_7", 1547718202, 6.720945201171228),Sensor("sensor_10", 1547718205, 38.101067604893444)))

    dataStream.print("sensor").setParallelism(1)
    env.execute("sensorTest")
  }
}
