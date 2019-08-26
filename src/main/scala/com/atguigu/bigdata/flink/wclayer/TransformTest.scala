package com.atguigu.bigdata.flink.wclayer

import java.{lang, util}

import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(List(1,2,6,3,4,5))
    val mds = ds.flatMap(ele=>List(ele,ele))
//    mds.print()
    val dso = env.fromCollection(List(Sensor("sensor_1",1547718199, 35.80018327300259),Sensor("sensor_6", 1547718201, 15.402984393403084),Sensor("sensor_7", 1547718202, 6.720945201171228),Sensor("sensor_10", 1547718205, 38.101067604893444)))

    val filterDS = dso.filter(new FilterFunction[Sensor] {
      override def filter(value: Sensor): Boolean = {
        value.id.contains("1")
      }
    })
//    filterDS.print()

    val line = env.readTextFile("input")
    val flatDS = line.flatMap(new FlatMapFunction[String, String] {
      //      override def flatMap(value: String, out: Collector[O]): Unit = ???
      override def flatMap(value: String, out: Collector[String]): Unit = {
        val strings = value.split(" ")
        strings.foreach(ele => {
          out.collect(ele)
        })
      }
    })
    val mapDS = flatDS.map(new MapFunction[String, (String, Int)] {
      override def map(value: String): (String, Int) = {
        (value, 1)
      }
    })

    val keyDS = mapDS.keyBy(new KeySelector[(String, Int), String] {
      override def getKey(value: (String, Int)): String = {
        value._1
      }
    })
//    keyDS.sum(1).print()
//    keyDS.max(1).print()
    val reduce = keyDS.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1 + value2._1, value1._2 + value2._2)
      }
    })

    val splitDS = flatDS.split(new OutputSelector[String] {
      override def select(out: String): lang.Iterable[String] = {
        //        var result= new util.ArrayList[String]()
        if (out.startsWith("he")) {
          val strings = new util.ArrayList[String]()
          strings.add("hello")
          strings
        } else {
          val strings = new util.ArrayList[String]()
          strings.add("others")
          strings
        }

      }
    })
//    splitDS.select("hello").print()
    val others = splitDS.select("others")

    val connectDS = dso.connect(flatDS)
    val mapConn = connectDS.map(new CoMapFunction[Sensor, String, (String, Int)] {
      override def map1(in1: Sensor): (String, Int) = {
        (in1.id, 1)
      }

      override def map2(in2: String): (String, Int) = {
        (in2, 1)
      }
    })
//    mapConn.print()
    val abcDS = env.fromCollection(List("a","b","c","d","d"))

    val unionDS = abcDS.union(flatDS)
    val unionResult = unionDS.map((_,1)).keyBy(0).sum(1)
//    unionResult.print()

    val rich = unionDS.map(new MyRich)

    rich.print()

    env.execute()

  }

}
class MyRich extends RichMapFunction[String,(String,Int)]{
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def map(value: String): (String, Int) = {
    (value,1)
  }
}
