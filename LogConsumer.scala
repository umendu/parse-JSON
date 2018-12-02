//package info.hadooptutorial.kafka.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import kafka.serializer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row;

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put

object LogConsumer {
  def main(args: Array[String]) {

    //Edit the following parameters
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("test")
    val tableName = "kafka_spark"
    val ssc = new StreamingContext(new SparkConf().setAppName("LogConsumer").setMaster("local[*]"), Seconds(10))
    // hostname:port for Kafka brokers, not Zookeeper

    val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParams, Set("test")).map(_._2)

    for (c <- lines) {
      // print(c.collect().toList)
      val f = c.collect().toList
      for (p <- f) {
        //println(p)
        val dataArray = p.split('|')
        val conf = HBaseConfiguration.create()
        val myTable = new HTable(conf, tableName)

        //val columnFamily = "cf"

        //	var p = new Put();
        val timestamp: Long = System.currentTimeMillis
        var put = new Put(new String(timestamp.toString()).getBytes())

        put.add("cf".getBytes(), "file_name".getBytes(), new String(dataArray(0)).getBytes());
        put.add("cf".getBytes(), "records_written".getBytes(), new String(dataArray(1)).getBytes());
        put.add("cf".getBytes(), "Date".getBytes(), new String(dataArray(2)).getBytes());
        myTable.put(put);

      }

    }

    ssc.start();
    ssc.awaitTermination();

  }
}
