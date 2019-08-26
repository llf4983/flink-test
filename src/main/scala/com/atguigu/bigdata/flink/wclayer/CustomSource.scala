package com.atguigu.bigdata.flink.wclayer

import java.util.Random

import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

object CustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val mySourceDataStream = env.addSource(new MySource())
    mySourceDataStream.print()
    env.execute()
  }

}
class MySource() extends SourceFunction[Sensor]{

  var flag=true
  override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
    val random = new Random()
    val tuples = 1.to(10).map(ele => {
      ("sensor_" + ele, 60 + random.nextGaussian() * 10)
    })

    while (flag){
//      var id="sensor_"+random.ints()
      val tuples1 = tuples.map(ele => {
        (ele._1, ele._2 + random.nextGaussian())
      })
      var time=System.currentTimeMillis()
      tuples1.foreach(ele=>{
        val sensor = new Sensor(ele._1,time,ele._2)
        sourceContext.collect(sensor)
      })
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    flag=false
  }
}

//class NewSource extends RichSourceFunction{
//  override def open(parameters: Configuration): Unit = super.open(parameters)
//
//  override def close(): Unit = super.close()
//
//  override def run(sourceContext: SourceFunction.SourceContext[Nothing]): Unit = ???
//
//  override def cancel(): Unit = ???
//}