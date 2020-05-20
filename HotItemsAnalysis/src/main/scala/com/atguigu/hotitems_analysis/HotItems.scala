package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 因为日志里面的时间戳为秒，所以此处乘以1000则变为毫秒

    // 3. transform 处理数据.
    val processedStream: DataStream[String] = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 只有紧跟在window之后的第一个算子才是得到window里面的全部event然后进行处理。其处理之后返回的结果为DataStream。
      // 然后将处理的结果发给下游的算子。因为好几个窗口可能是同时进行的，所以下游的算子可能同时输入好几个window输出的结果
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合，第一个函数的预聚和的结果，传递给第二个函数
      .keyBy(_.windowEnd) // 按照窗口分组
      //在process函数里面得到一个key对应的全部event，然后进行排序。因为process函数是最底层的函数。
      // 所以可以进行有状态的编程
      //将一个key对应的全部数据存储在list状态里面。然后等收集完一个key所有的event的时候，再进行排序
      .process(new TopNHotItems(3))




    // 4. sink：控制台输出
    processedStream.print()

    env.execute("hot items job")
  }

}

// 自定义预聚合函数,只要窗口里面新来一条key对应的pv数据，则将对应的key的count数量加1
// 泛型UserBehavior是输入的event的类型. 第二个泛型Long是累加其器的类型
// 第三个泛型Long是输出的event类型
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，输出ItemViewCount，即定义自己的输出类型，
// 将第一个函数聚合的结果做为ItemViewCount对应的字段
// [IN, OUT, KEY, W <: Window]
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }


  //  不使用processElement的参数out则表示此处不进行任何输出操做，而此时则一般需要在定时器里面定义输出的结果
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    // 按照count大小排序，并取前N个
    val sortedItems: mutable.Seq[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每一个商品的信息
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("================================")
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

