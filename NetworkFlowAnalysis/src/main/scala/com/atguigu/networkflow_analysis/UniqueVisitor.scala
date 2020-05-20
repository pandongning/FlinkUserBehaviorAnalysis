package com.atguigu.networkflow_analysis

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// windowEnd为窗口的信息，uvCount该窗口里面的uv数量
case class UvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[UvCount] = env.readTextFile(resource.getPath)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计pv操作
      .timeWindowAll(Time.hours(1)) //此处必须定义一个窗口，用于统计该窗口里面的uv数量,因为流式处理数据是无界的，不可能在无界的数据集上面统计所有的结果
      .apply(new UvCountByWindow())

    dataStream.print()
    env.execute("uv job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  //  input为该窗口的所有数据
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet: Set[Long] = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }

}
