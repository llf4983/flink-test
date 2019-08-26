package com.atguigu.bigdata.flink.wclayer

import org.apache.flink.api.scala._


object BatchMode {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val lineDS = environment.readTextFile("input","UTF-8")

    val result = lineDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    result.print()
  }
}
