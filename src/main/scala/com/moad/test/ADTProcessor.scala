package com.moad.test

import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import java.util.HashMap

import com.elsevier.spark_xml_utils.xpath.XPathProcessor

object ADTProcessor {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Max Price")
    val sc = new SparkContext(conf)

    try{

      val rawdata = sc.sequenceFile[String,String](args(0))
      val processed = rawdata.mapPartitions(partition =>{
            val xmsgSender = "string(/NS1:HL7/NS1:MSH/NS1:MSH.3.SendingApplication/NS1:HD.1.NamespaceId)"
            val xmsgType = "concat(string(/NS1:HL7/NS1:MSH/NS1:MSH.9.MessageType/NS1:MSG.1.MessageType),string(/NS1:HL7/NS1:MSH/NS1:MSH.9.MessageType/NS1:MSG.2.TriggerEvent))"
            val xmsgId = "string(/NS1:HL7/NS1:MSH/NS1:MSH.10.MessageControlID)"
            val namespaces = new util.HashMap[String,String](Map(
                "NS1" -> "urn:upmc-edu:v2xml"
            ).asJava)



            val senderVal = XPathProcessor.getInstance(xmsgSender,namespaces)
            val typeVal = XPathProcessor.getInstance(xmsgType, namespaces)
            val idVal = XPathProcessor.getInstance(xmsgId,namespaces)

            partition.map(rec =>{
                val sender = senderVal.evaluateString(rec._2)
                val msgtype = typeVal.evaluateString(rec._2)
                val id = idVal.evaluateString(rec._2)
              (sender, msgtype, id)
            })
      })

      processed.saveAsTextFile(args(1))

    }
    catch{
      case e: Exception => e.printStackTrace()

    }
    finally {
      sc.stop()
    }
  }
}
