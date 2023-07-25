package com.bjsxt.traffic.utils

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, client}
/**
  *  HBase sink ,批量数据插入到HBase中
  */
class HBaseSink extends RichSinkFunction[java.util.List[Put]] {
  var configuration: conf.Configuration = _
  var conn: client.Connection = _
  //初始化 RichSinkFunction 对象时 执行一次
  override def open(parameters: Configuration): Unit = {
    configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","mynode3:2181,mynode4:2181,mynode5:2181")
    conn = ConnectionFactory.createConnection(configuration)
  }

  //每条数据执行一次
  override def invoke(value: util.List[Put], context: SinkFunction.Context[_]): Unit = {
    //连接HBase 表
    val table: Table = conn.getTable(TableName.valueOf("a1"))
    table.put(value)
  }
}
