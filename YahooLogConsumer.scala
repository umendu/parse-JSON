package info.yahoo.finance

import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import kafka.serializer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row;
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


object YahooLogConsumer {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCount <zkQuorum>")
      System.exit(1)
    }
val Array(zkQuorum,topicNames,tableNameVal) = args;
    //Edit the following parameters
    val kafkaParams = Map("zkQuorum" -> zkQuorum)
    
    val tableName = tableNameVal
    val sparkCOnf=new SparkConf().setAppName("LogConsumer").setMaster("local[*]")
    val sc=new SparkContext(sparkCOnf);
   val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
     /*sqlContext.setConf("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
    sqlContext.setConf("spark.sql.hive.thriftServer.singleSession", "true")
    sqlContext.setConf("hive.metastore.sasl.enabled", "true")
    System.setProperty("hive.security.authorization.enabled", "false");
    System.setProperty("hive.metastore.execute.setugi", "true");
    val ssc = new StreamingContext(sparkCOnf,Seconds(10))*/
    // hostname:port for Kafka brokers, not Zookeeper
    
    val ssc = new StreamingContext(sparkCOnf,Seconds(10))
    //val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParams, topicNames).map(_._2)
    
    val lines = KafkaUtils.createStream(ssc, zkQuorum,"kafka-group", Map(topicNames->5)).map(_._2)
    //var con = getConnection()
    //var statement: Statement = null
    //if (con != null) {
    //  statement = con.createStatement()
    //}

    for (c <- lines) {
      
      val f = c.collect().toList
      for (p <- f) {
       
        val dataArray = p.split('|')
//        val insertstmt = "INSERT INTO TABLE spark_stream.spark_table VALUES('"+dataArray(0)+"', '"+ dataArray(1)+"')"
//       println(insertstmt)
//        sqlContext.sql(insertstmt)
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "dayrhegapd014.enterprisenet.org:2181,dayrhegapd015.enterprisenet.org:2181,dayrhegapd016.enterprisenet.org:2181,dayrhegapd019.enterprisenet.org:2181,dayrhegapd020.enterprisenet.org:2181");
//                if (statement != null) {
//                  statement.executeQuery("INSERT INTO kafka_spark VALUES('" + new String(dataArray(0)) + "','" + new String(dataArray(1)) + "','" + new String(dataArray(2)) + "','" + new String(dataArray(3)) + "','" + new String(dataArray(4)) + "')");
//                }
        val myTable = new HTable(conf, tableName)
        val timestamp: Long = System.currentTimeMillis
        var put = new Put(new String(timestamp.toString()).getBytes())
        
        put.add("cf".getBytes(), "Timestamp".getBytes(), new String(timestamp.toString()).getBytes());
        put.add("cf".getBytes(), "Stock_Name".getBytes(), new String(dataArray(0)).getBytes());
        put.add("cf".getBytes(), "currency".getBytes(), new String(dataArray(1)).getBytes());
        put.add("cf".getBytes(), "dividend".getBytes(), new String(dataArray(2)).getBytes());
        put.add("cf".getBytes(), "Price".getBytes(), new String(dataArray(3)).getBytes());
        put.add("cf".getBytes(), "Stats".getBytes(), new String(dataArray(4)).getBytes());
        myTable.put(put);
        println("Completed insertion")

      }

    }

    ssc.start();
    ssc.awaitTermination();

  }
   def getConnection(): Connection = {
    try {
      // Register driver and create driver instance
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch {
      case e: ClassNotFoundException => e.printStackTrace();
    }
    val con = DriverManager.getConnection("jdbc:hive2://dayrhegapd016.enterprisenet.org:10000/");
    // create statement

    return con;
  }

}
