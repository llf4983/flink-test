package com.atguigu.bigdata.flink.wclayer.window

import com.atguigu.bigdata.flink.wclayer.MySource
import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//      env.setMaxParallelism(1)
//    val sensorDS = env.addSource(new MySource)
    val lineDS = env.readTextFile("input2")
    val sensorDS = lineDS.map(ele => {
      val strings = ele.split(",")
      new Sensor(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })
    val tuple = sensorDS.map(ele=>(ele.id,ele.temperature))
    val win = tuple.keyBy(0).countWindow(5)
    val windowDS = win.reduce((ele1,ele2)=>(ele1._1,ele1._2.max(ele2._2)))
    windowDS.print()
    println("***********************")

    env.execute()

  }
}
