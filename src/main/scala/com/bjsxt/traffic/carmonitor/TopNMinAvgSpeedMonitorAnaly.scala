package com.bjsxt.traffic.carmonitor

import java.util.Properties

import com.bjsxt.traffic.utils._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 每隔1分钟统计最近5分钟 最通畅的topN卡扣信息
  */
object TopNMinAvgSpeedMonitorAnaly {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //设置并行度和事件时间
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取Kafka中 实时监控车辆数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","mynode1:9092,mynode2:9092,mynode3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group112502")

    //读取Kafka 中监控到实时的车辆信息
    val monitorInfosDs: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("monitortopic1125",new SimpleStringSchema(),props).setStartFromEarliest())
//    val monitorInfosDs: DataStream[String] = env.socketTextStream("mynode5",9999)
    //数据类型转换
    val transferDS: DataStream[MonitorCarInfo] = monitorInfosDs.map(line => {
      val arr: Array[String] = line.split("\t")
      MonitorCarInfo(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5), arr(6).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MonitorCarInfo](Time.seconds(5)) {
      override def extractTimestamp(element: MonitorCarInfo): Long = element.actionTime
    })

    //每隔1分钟统计过去5分钟，最通畅的top5卡扣信息
    val top5MonitorInfoDS: DataStream[Top5MonitorInfo] = transferDS.timeWindowAll(Time.minutes(5), Time.minutes(1))
      .process(new ProcessAllWindowFunction[MonitorCarInfo, Top5MonitorInfo, TimeWindow] {
        //context :Flink 上下文，elements : 窗口期内所有元素，out: 回收数据对象
        override def process(context: Context, elements: Iterable[MonitorCarInfo], out: Collector[Top5MonitorInfo]): Unit = {
          val map = scala.collection.mutable.Map[String, MonitorSpeedClsCount]()
          val iter: Iterator[MonitorCarInfo] = elements.iterator
          while (iter.hasNext) {
            val currentInfo: MonitorCarInfo = iter.next()
            val areaId: String = currentInfo.areaId
            val roadId: String = currentInfo.roadId
            val monitorId: String = currentInfo.monitorId
            val speed: Double = currentInfo.speed
            val currentKey = areaId + "_" + roadId + "_" + monitorId
            //判断当前map中是否含有当前本条数据对应的 区域_道路_卡扣的信息
            if (map.contains(currentKey)) {
              //判断当前此条车辆速度位于哪个速度端，给map中当前key 对应的value MonitorSpeedClsCount 对象对应的速度段加1
              if (speed >= 120) {
                map.get(currentKey).get.hightSpeedCarCount += 1
              } else if (speed >= 90) {
                map.get(currentKey).get.middleSpeedCount += 1
              } else if (speed >= 60) {
                map.get(currentKey).get.normalSpeedCarCount += 1
              } else {
                map.get(currentKey).get.lowSpeedCarCount += 1
              }
            } else {
              //不包含 当前key
              val mscc = MonitorSpeedClsCount(0L, 0L, 0L, 0L)
              if (speed >= 120) {
                mscc.hightSpeedCarCount += 1
              } else if (speed >= 90) {
                mscc.middleSpeedCount += 1
              } else if (speed >= 60) {
                mscc.normalSpeedCarCount += 1
              } else {
                mscc.lowSpeedCarCount += 1
              }
              map.put(currentKey, mscc)
            }
          }

          val tuples: List[(String, MonitorSpeedClsCount)] = map.toList.sortWith((tp1, tp2) => {
            tp1._2 > tp2._2
          }).take(5)
          for (elem <- tuples) {
            val windowStartTime: String = DateUtils.timestampToDataStr(context.window.getStart)
            val windowEndTime: String = DateUtils.timestampToDataStr(context.window.getEnd)
            out.collect(new Top5MonitorInfo(windowStartTime, windowEndTime, elem._1, elem._2.hightSpeedCarCount, elem._2.middleSpeedCount, elem._2.normalSpeedCarCount, elem._2.lowSpeedCarCount))
          }

        }
      })

    //打印结果

    top5MonitorInfoDS.addSink(new JDBCSink[Top5MonitorInfo]("Top5MonitorInfo"))
    env.execute()

  }

}
