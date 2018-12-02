package com.java.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Result

object HbaseScalaExample {
  def main(args: Array[String]): Unit = {
    val csId = 98

    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.quorum", "dayrhegapd014.enterprisenet.org,dayrhegapd015.enterprisenet.org,dayrhegapd016.enterprisenet.org")
    val myTable = new HTable(hConf, "customer")

    val scan = new Scan

    val scanner = myTable.getScanner(scan)

    println("Scan 1234: " + scanner.next().getColumnCells("cf".getBytes, "cust_data".getBytes))

    val g = new Get("98".getBytes)
    val result = myTable.get(g)
    println(result.getColumn("cf".getBytes, "cust_data".getBytes))
   // println("Gettttttttttt: " + result.getValue("cf".getBytes, "cust_data".getBytes).toString())
   // println(result.getMap)
    if (!result.isEmpty()) {
      println(result.value())
    } else {
      println("Its empty")
    }
  }
}

