package com.atguigu.loginfail_detect

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取事件数据，创建简单事件流
    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: KeyedStream[LoginEvent, Long] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId) //因为此处进行了keyBy，所以自己则以后的处理中，则只需要认为自己只处理某个key对应的流

    // 2. 定义匹配模式 3秒之内连续失败两次

    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where((_: LoginEvent).eventType == "fail")
      .within(Time.seconds(3))

    // 3. 在事件流上应用模式，得到一个pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 从pattern stream上应用select function，检出匹配事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute("login fail with cep job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  //  map里面存放检测到的事件对应的模式名字和对应的事件，因为有的模式为loop，所以可能对应多个事件
  //  所以将其放在List里面
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
    // val iter = map.get("begin").iterator()
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")
  }
}