package com.atguigu.bigdata.flink.wclayer.sink

import com.atguigu.bigdata.flink.wclayer.MySource
import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ESSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sensorSource = env.addSource(new MySource)

    val list = new java.util.ArrayList[HttpHost]()
    list.add(new HttpHost("hadoop102",9200))
    val esBuilder = new ElasticsearchSink.Builder[Sensor](list,new Es())
    sensorSource.addSink(esBuilder.build())

    env.execute()

  }
}
class Es extends ElasticsearchSinkFunction[Sensor]{
  override def process(element: Sensor, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    val resultMap = new java.util.HashMap[String,String]()
    resultMap.put("dataTest",element.toString)
    val indexRequest = Requests.indexRequest().index("sensor").`type`("_doc").source(resultMap)
    indexer.add(indexRequest)
  }
}