package com.atguigu.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单事件流
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // join处理
    val processedStream = orderEventStream
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()

    env.execute("tx pay match by join job")
  }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //  此处只能得到join上的数据，对于join不上的，是无法得到的，但是此时的需求是获取无法join上面的数据
  //  即获取用户已经支付成功，但是第三方支付平台还未到账的消息
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
