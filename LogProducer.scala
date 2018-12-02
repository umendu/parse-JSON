//package info.hadooptutorial.kafka.spark

import java.util.HashMap

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.commons.io.input.ReversedLinesFileReader
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.io.Source
import scala.util.matching.Regex.Match
import scala.util.matching.Regex
import java.util.regex.Pattern
import java.io.RandomAccessFile

object LogProducer {

  def main(args: Array[String]): Unit = {
    
    //Edit the following  Variables.....
    val brokers = "localhost:9092"
    val topic = "test"
    var dir: String = "/home/cloudera/workspace_scala/Kafka_SparkStreaming_LogProcessor/Input/"
    var ext: String = "abc1"

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    var date: String = ""
    var filename: String = ""
    var norecords: String = ""

    //println(dir)

    val files = recursiveListFiles(new java.io.File(dir))

    for (file <- files if file.getName startsWith ext) {
      //println(file+"-----------------------"+file.getName)
      val headerDate = new ReversedLinesFileReader(file)
      val hDate = headerDate.readLine()
      val datepattern = Pattern.compile("HEADER_DATE=([0-9]*?)$")
      val datematcher = datepattern.matcher(hDate)
      if (datematcher.find) {
        date = datematcher.group(1)
        println("Inside Header Date:--->" + date)
      }
      //println("Header Date:--->"+date)
      for (line <- Source.fromFile(file).getLines) {

        val filepattern = Pattern.compile("FILE_NAME=([\\w\\W]*?)\\|")
        val filematcher = filepattern.matcher(line)
        if (filematcher.find) {
          filename = filematcher.group(1)
        }
        val writtenpattern = Pattern.compile("WRITTEN=([\\w\\W]*?)$")
        val writtenmatcher = writtenpattern.matcher(line)
        if (writtenmatcher.find) {
          norecords = writtenmatcher.group(1)
        }
        if (!filename.isEmpty()) {
          var values = filename + "|" + norecords + "|" + date

          println(values + "======values")
          val message = new ProducerRecord[String, String](topic, null, values)
          filename = ""
          println(message + "======message")
          producer.send(message)
        }
      }
    }
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

}