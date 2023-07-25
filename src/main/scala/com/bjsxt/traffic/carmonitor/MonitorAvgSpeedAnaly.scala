package com.bjsxt.traffic.carmonitor

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.bjsxt.traffic.utils.{JDBCSink, MonitorAvgSpeedInfo, MonitorCarInfo}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  *  每隔1分钟统计过去5分钟，每个区域每个道路每个卡扣的平均速度
  */
object MonitorAvgSpeedAnaly {
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


    //设置窗口，每隔1分钟计算每个卡扣的平均速度,使用aggregate 增量计算+全量计算
    val monitorAvgSpeedDS: DataStream[MonitorAvgSpeedInfo] = transferDS.keyBy(mi => {
      mi.areaId + "_" + mi.roadId + "_" + mi.monitorId
    }).timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate(new AggregateFunction[MonitorCarInfo, (Long, Double), (Long, Double)] {
        override def createAccumulator(): (Long, Double) = (0L, 0L)

        override def add(value: MonitorCarInfo, accumulator: (Long, Double)): (Long, Double) = {
          (accumulator._1 + 1, accumulator._2 + value.speed)
        }

        override def getResult(accumulator: (Long, Double)): (Long, Double) = accumulator

        override def merge(a: (Long, Double), b: (Long, Double)): (Long, Double) = (a._1 + b._1, a._2 + b._2)
      },
        new WindowFunction[(Long, Double), MonitorAvgSpeedInfo, String, TimeWindow] {
          //key : 区域_道路_卡扣  window : 当前窗口，input ：输入的数据 ，out:回收数据对象
          override def apply(key: String, window: TimeWindow, input: Iterable[(Long, Double)], out: Collector[MonitorAvgSpeedInfo]): Unit = {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val startTime: String = sdf.format(new Date(window.getStart))
            val endTime: String = sdf.format(new Date(window.getEnd))
            //计算当前卡扣的平均速度
            val last: (Long, Double) = input.last
            val avgSpeed: Double = (last._2 / last._1).formatted("%.2f").toDouble
            out.collect(new MonitorAvgSpeedInfo(startTime, endTime, key, avgSpeed, last._1))
          }
        })

    //保存到mysql 数据库中
    monitorAvgSpeedDS.addSink(new JDBCSink[MonitorAvgSpeedInfo]("MonitorAvgSpeedInfo"))

    env.execute()




  }

}
