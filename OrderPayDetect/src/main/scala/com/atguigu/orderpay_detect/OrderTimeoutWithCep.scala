package com.atguigu.orderpay_detect

import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义一个匹配模式 followedBy为非严格模式，表示允许create和pay之间插入其他类型的操做，比如修改订单
    //    下面虽然定义规则应用于15分钟。但是只要在15分钟之内也检测到了符合规则的事件，其也会输出
    //    并不是一定要等到15分钟之后才输出
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 把模式应用到stream上，得到一个pattern stream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    // 此处主要需要提取的就是超时的事件，即15分钟内没有支付的订单事件
    val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")


    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    //直接输出正常的流里面的event
    resultStream.print("payed")
    //    输出超时事件的流
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    //    对于超时的事件，其只有begin，没有follow，所以此处只能获取begin
    val timeoutOrderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
