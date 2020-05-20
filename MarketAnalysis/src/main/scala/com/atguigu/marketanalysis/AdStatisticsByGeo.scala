package com.atguigu.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

// 输出的黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据并转换成AdClickEvent
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义process function，过滤大量刷点击的行为。
    // 个人觉得显示当中一个人对一个广告同一天的点击大于15次，就可以加入到黑名单了
    // 但是此处不能使用天做为窗口的size去进行计算，因为其必须收集到一天的数据之后，才会触发窗口的计算，所以其
    // 延时太大，但是此处需要做到实时的统计点击的次数，所以必须是来一条数据就处理一次
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100)) //此处process的输入时经过keyBy之后的流。所以在处理的时候，则认为接受到的每个元素都是同一个key对应的event


    // 根据省份做分组，开窗聚合. 得到此窗口里面的每个省的广告点击总量
    val adCountStream: DataStream[CountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")

    //应该将SideOutput的内容写入到mysql
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    env.execute("ad statistics job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送过黑名单的状态。因为对于一个用户，只需要发送一次其黑名单信息给侧输出流即可。
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {

      // 取出count状态
      val curCount: Long = countState.value()

      // 如果是今天第一次处理，注册定时器，每天00：00触发。清空上一天的所有状态信息
      if (curCount == 0) {
        // ts=24小时
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果到达则加入黑名单
      if (curCount >= maxCount) {
        // 判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
        }
        return
      }
      // 计数状态加1，输出数据到主流
      countState.update(curCount + 1)
      out.collect(value)
    }

//    由于是每天重新，所以必须每天定时清空状态信息
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

}


// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


// 自定义窗口处理函数
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}
