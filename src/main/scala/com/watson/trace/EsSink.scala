package com.watson.trace

import java.net.InetAddress

import com.watson.UnifiedTraceSchemaCase
import com.watson.util.DateUtils
import org.apache.spark.sql.ForeachWriter
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * EsSink
  *
  * Created by watson on 2018/9/5.
  */
class EsSink(host: String, port: String) extends ForeachWriter[UnifiedTraceSchemaCase] {

  var client: TransportClient = _

  override def open(partitionId: Long, version: Long): Boolean = {

    if (client == null) {

      System.setProperty("es.set.netty.runtime.available.processors", "false")
      val settings = Settings.builder()
        .put("cluster.name", "general")
        .put("client.transport.sniff", false)
        .put("client.transport.ping_timeout", "15s")
        .put("client.transport.nodes_sampler_interval", "15s")
        .put("transport.type", "netty3") //防止版本冲突，使用netty3
        .put("http.type", "netty3")
        .build()

      client = new PreBuiltTransportClient(settings)

      for (eachHost <- host.split(";")) {
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(eachHost), Integer.valueOf(port)))
      }
    }

    true
  }

  override def process(value: UnifiedTraceSchemaCase): Unit = {
    try {
      val indexName = "trace-" + DateUtils.getCurrentTime2String(DateUtils.YYYYMMDD, System.currentTimeMillis())
      val indexRequest = Requests.indexRequest(indexName)
        .`type`("span")
        .source(value.trace, XContentType.JSON)

      val resp = this.client.index(indexRequest)
      //等待数据保存成功
      resp.actionGet(5000)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    this.client.close()
  }
}