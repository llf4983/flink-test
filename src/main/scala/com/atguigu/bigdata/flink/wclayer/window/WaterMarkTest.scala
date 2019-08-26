package com.atguigu.bigdata.flink.wclayer.window

import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.getConfig.setAutoWatermarkInterval(100L)

    //    val stream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val stream = env.socketTextStream("hadoop102", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      Sensor(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000)
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(1)) {
      override def extractTimestamp(element: Sensor): Long = element.timeStamp * 1000
    })

    val minTempPerWindowByEventTimeDS: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      //得到KeyedStream
      .keyBy(_._1)
      //对10s内相同id的数据取时间最近温度最小值
      .timeWindow(Time.seconds(10))
      //使用reduce做增量聚合,回到DataStream
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    //		minTempPerWindowByProcessingTimeDS.print("min temp")
    minTempPerWindowByEventTimeDS.print("min temp")
    println()
    dataStream.print("input data")


    env.execute("Window Test")
  }
}

//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
//  val bound = 60000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(maxTs-bound)
//
//  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
//    maxTs = maxTs.max(element.timestamp* 1000)
//    element.timestamp * 1000
//  }
//}

//class MyAssigner() extends AssignerWithPunctuatedWatermarks[Sensor]{
//  override def checkAndGetNextWatermark(lastElement: Sensor, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)
//
//  override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = element.timeStamp * 1000
//}

