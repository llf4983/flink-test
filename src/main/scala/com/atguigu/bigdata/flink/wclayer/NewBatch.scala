package com.atguigu.bigdata.flink.wclayer

import org.apache.flink.api.scala.ExecutionEnvironment

object NewBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val lineDataSet = env.readTextFile("input","UTF-8")
    import org.apache.flink.api.scala._
//    val groupDataSet = lineDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(ele=>ele._1)
    val groupDataSet = lineDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0)
    groupDataSet.sum(1).print()
//    val list =(1,2,3,4,5,6)

//    println(list.productElement(5))
  }
}
