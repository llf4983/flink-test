package com.atguigu.bigdata.flink.wclayer

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object NewStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lineDataSteeam = env.socketTextStream("hadoop102",7777)
    import org.apache.flink.streaming.api.scala._
    val result = lineDataSteeam.flatMap(_.split(" ")).map((_,1)).keyBy(_._1).sum(1)
    result.print()

    env.execute("new Stream")


  }
}
