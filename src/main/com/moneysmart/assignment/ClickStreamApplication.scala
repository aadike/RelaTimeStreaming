package com.moneysmart.assignment

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.json4s._
import org.json4s.native.JsonMethods._

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.{HTable, Put};
import org.apache.hadoop.hbase.util.Bytes;


Object ClickStreamApplication {
	def main(args: Array[String]): Unit = {
	
		// Creating streaming context object
		var conf = new SparkConf().setMaster("local[*]").setAppName("CLICK_STREAM_DATA")
		val ssc = new StreamingContext(conf, Seconds(5));
		
		// kafka properties
		var props = new Properties
		props.put("bootstrap.servers", "localhost:9092")
		props.put("key.deserializer","org.apache.kafka.common.serilization.StringDeserializer")
		props.put("value.deserializer","org.apache.kafka.common.serilization.StringDeserializer")
		props.put("group.id","moneysmart")
		
		// kafka topic name
		val topics = Arrays.asList("clickstreamdata")
		
		val kafkaStream = KafkaUtils.createDirectStream[String,String](
									ssc, 
									Subscribe[String, String](topics,props))
		val inputMsg = kafkaStream.map(record => record.value.toString)
		val webClickData = parseMessage(inputMsg)
		
		// Hbase configuration
		ssc.checkpoint(directory="hdfs://quickstart.cloudera:8020/WebClickData")
		
		inputMsg.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(toHbase(_))
		
		// start application
		ssc.start()
		ssc.awaitTermination()
	}
	
	// write to Hbase
		def toHbase(row: (_)): Unit = {
			val hConf = new HBaseConfiguration
			hConf.set("hbase.zookeeper.quorum" "localhost:2181")
			val tableName = "WebClickInfo"
			va hTable = new HTable(hConf, tableName)
			val p = new Put(Bytes.toBytes(webClickData.user_id))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("session_id"),Bytes.toBytes(webClickData.session_id))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("country"),Bytes.toBytes(webClickData.country))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("user_agent"),Bytes.toBytes(webClickData.user_agent))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("version"),Bytes.toBytes(webClickData.version))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("language"),Bytes.toBytes(webClickData.language))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("date"),Bytes.toBytes(webClickData.date))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("partner_id"),Bytes.toBytes(webClickData.partner_id))
			p.add(Bytes.toBytes("userinfo"), Bytes.toBytes("partner_name"),Bytes.toBytes(webClickData.partner_name))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("event"),Bytes.toBytes(webClickData.event))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("cart_amount"),Bytes.toBytes(webClickData.cart_amount))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("search_query"),Bytes.toBytes(webClickData.search_query))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("current_url"),Bytes.toBytes(webClickData.current_url))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("category"),Bytes.toBytes(webClickData.category))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("referrer"),Bytes.toBytes(webClickData.referrer))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("init_session"),Bytes.toBytes(webClickData.init_session))
			p.add(Bytes.toBytes("clickinfo"), Bytes.toBytes("page_type"),Bytes.toBytes(webClickData.page_type))
			hTable.put(p)
		}
		
	case class WebClick(session_id: String,
				event: String,
				partner_id: String,
				partner_name: String,
				cart_amount: Int,
				country: String,
				user_agent: String,
				user_id: String,
				version: String,
				language: String,
				date: Int,
				search_query: String,
				current_url: String,
				category: String,
				referrer: String,
				init_session: Boolean,
				page_type: String) extends Serializable
				
	def parseMessage(jsonString: String): WebClick = {
		// using lift json library (org.json4s)
		val msg = parse(jsonString).extract[WebClick]
		return msg
	}
}