package com.atguigu.bigdata.flink.wclayer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamMode {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val param = ParameterTool.fromArgs(args)
    val hostname = param.get("hostname")
    val port = param.getInt("port")


    val textDSteam = env.socketTextStream(hostname,port)
    import org.apache.flink.api.scala._
    val re = textDSteam.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1))
    val value = re.keyBy(0)
    val result = value.sum(1)
    result.print()

    env.execute("wccccc")

  }
}