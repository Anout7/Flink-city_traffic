package com.bjsxt.traffic.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class JDBCSink[T](cls:String) extends RichSinkFunction[T] {
  var conn: Connection = _
  var pst: PreparedStatement = _
  var stop = false
  //当初始化 RichSinnkFunction时，只会调用一次
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://192.168.179.14:3306/traffic_monitor", "root", "123456")
  }

  //来一条数据，处理一次
  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    if("OverSpeedCarInfo".equals(cls)){
      //统计超速车辆信息
      val info: OverSpeedCarInfo = value.asInstanceOf[OverSpeedCarInfo]
      pst = conn.prepareStatement("insert into t_speeding_info (car,monitor_id,road_id,real_speed,limit_speed,action_time) values(?,?,?,?,?,?)")
      pst.setString(1,info.car)
      pst.setString(2,info.monitorId)
      pst.setString(3,info.roadId)
      pst.setDouble(4,info.realSpeed)
      pst.setDouble(5,info.limitSpeed)
      pst.setLong(6,info.actionTime)
      pst.executeUpdate()
    }else if("MonitorAvgSpeedInfo".equals(cls)){
      //计算每个卡扣平均速度
      val info: MonitorAvgSpeedInfo = value.asInstanceOf[MonitorAvgSpeedInfo]
      pst = conn.prepareStatement("insert into t_average_speed (start_time,end_time,monitor_id,avg_speed,car_count) values(?,?,?,?,?)")
      pst.setString(1,info.windowStartTime)
      pst.setString(2,info.windowEndTime)
      pst.setString(3,info.monitorId)
      pst.setDouble(4,info.avgSpeed)
      pst.setLong(5,info.carCount)
      pst.executeUpdate()

    }else if("Top5MonitorInfo".equals(cls)){
      //统计最通畅的top5卡扣
      val info: Top5MonitorInfo = value.asInstanceOf[Top5MonitorInfo]
      pst = conn.prepareStatement("insert into t_top5_monitor_info (start_time,end_time,monitor_id,hight_speed_carcount,middle_speed_carcount,normal_speed_carcount,low_speed_carcount) values(?,?,?,?,?,?,?)")
      pst.setString(1,info.windowStartTime)
      pst.setString(2,info.windowEndTime)
      pst.setString(3,info.monitorId)
      pst.setLong(4,info.hightSpeedCarCount)
      pst.setLong(5,info.middleSpeedCount)
      pst.setLong(6,info.normalSpeedCarCount)
      pst.setLong(7,info.lowSpeedCarCount)
      pst.executeUpdate()
    }else if("ViolationCarInfo".equals(cls)){
      //统计 违法车辆-套牌车辆信息
      val info: ViolationCarInfo = value.asInstanceOf[ViolationCarInfo]
      pst = conn.prepareStatement("insert into t_violation_list (car,violation,create_time,detail) values(?,?,?,?)")
      pst.setString(1,info.car)
      pst.setString(2,info.violation)
      pst.setString(3,info.createTime)
      pst.setString(4,info.detail)
      pst.executeUpdate()
    }







  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}
