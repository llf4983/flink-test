package com.atguigu.bigdata.flink.wclayer.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bigdata.flink.wclayer.MySource
import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new MySource)
    source.addSink(new MyJDBCSink())

    env.execute()
  }
}
class MyJDBCSink() extends RichSinkFunction[Sensor]{
  var conn:Connection=_
  var insert:PreparedStatement=_
  var update:PreparedStatement=_

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn=DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink","root","000000")
    insert=conn.prepareStatement("insert into flinktable(sensor,tmp) values (?,?)")
    update=conn.prepareStatement("update flinktable set tmp=? where sensor=?")
  }

  override def close(): Unit = {
    insert.close()
    update.close()
    conn.close()
  }

  override def invoke(value: Sensor, context: SinkFunction.Context[_]): Unit = {
    update.setDouble(1,value.temperature)
    update.setString(2,value.id)
    update.execute()

    if(update.getUpdateCount()==0){
      insert.setString(1,value.id)
      insert.setDouble(2,value.temperature)
      insert.execute()
    }
  }
}
