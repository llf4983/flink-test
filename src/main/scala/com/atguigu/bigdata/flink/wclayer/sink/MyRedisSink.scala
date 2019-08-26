package com.atguigu.bigdata.flink.wclayer.sink

import com.atguigu.bigdata.flink.wclayer.MySource
import com.atguigu.bigdata.flink.wclayer.bean.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val DS = env.addSource(new MySource())
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    DS.addSink(new RedisSink[Sensor](conf,new MyRe()))
    env.execute()
  }
}

class MyRe extends RedisMapper[Sensor]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"SENSOR_TEST")
  }

  override def getKeyFromData(data: Sensor): String = {
    data.id
  }

  override def getValueFromData(data: Sensor): String = {
    data.temperature.toString
  }
}
