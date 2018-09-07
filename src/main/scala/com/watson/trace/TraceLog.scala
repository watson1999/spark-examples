package com.watson.trace

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.watson.UnifiedTraceSchemaCase
import com.watson.util.{DateUtils, GzipUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * TraceLog
  *
  * Created by manji on 2018/9/4.
  */
object TraceLog {

  def main(args: Array[String]) {

    val checkpoint = "/trace/checkpoint"
    val esHost = "127.0.0.1"
    val kafkaServers = "127.0.0.1:9092"

    val spark = SparkSession
      .builder
      .appName("TraceLog")
      .master("local[*]")
      .getOrCreate()

    // 订阅kafka topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", "trace")
      .load()


    import spark.implicits._

    val esResult = df.map(each => {
      var eachLine = ""
      try {
        eachLine = GzipUtils.decompress(each.getAs("value"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      println(eachLine)
      val json = new JSONObject()
      try {
        if (StringUtils.isNotBlank(eachLine)) {
          val trace = JSON.parseObject(eachLine)
          val tag = trace.getJSONObject("spanTag").getJSONObject("tag")
          json.put("traceId", trace.getString("traceId"))
          json.put("spanId", trace.getString("spanId"))
          json.put("concurrent", tag.getIntValue("concurrent"))
          json.put("elapsed", tag.getLongValue("elapsed"))
          json.put("serverName", tag.getString("serverName"))
          json.put("iface", tag.getString("iface"))
          json.put("version", tag.getString("version"))
          json.put("method", tag.getString("method"))
          json.put("input", tag.getString("input"))
          json.put("output", tag.getString("output"))
          json.put("kind", tag.getString("kind"))
          json.put("localAddress", tag.getString("localAddress"))
          json.put("remoteAddress", tag.getString("remoteAddress"))
          json.put("success", tag.getBooleanValue("success"))
          json.put("timestamp", tag.getLongValue("timestamp"))
          json.put("saveTime", DateUtils.getISO8601Timestamp(new Date()))
        }
      } catch {
        case e: Exception =>
          System.err.println(eachLine)
          e.printStackTrace()
      }
      UnifiedTraceSchemaCase(if (json.isEmpty) "" else json.toJSONString)
    })

    val esWriter = new EsSink(esHost, "9300")

    esResult.filter(result => !"".equals(result.trace)).writeStream
      .foreach(esWriter)
      .option("checkpointLocation", checkpoint)
      .start()

    spark.streams.awaitAnyTermination()
  }
}