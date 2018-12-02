package info.hadooptutorial.service

import java.util.Calendar
import scala.collection.mutable.ListBuffer
import org.codehaus.jackson.JsonParser
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.Extraction
import org.json4s.native.Serialization
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object CustomerService_Bkp {

  def main(args: Array[String]): Unit = {

    case class Details(
        var length_of_stay: String,
        var season: String,
        var reservation_status: String,
        var booking_date: String,
        var upcoming_reservation_date: String,
        var arrival_date: String,
        var departure_date: String) {
      def this() = this(null, null, null, null, null, null, null)
    }

    case class Upcoming_stays(
        var hotel_name: String,
        var details: ListBuffer[Details]) {
      def this() = this(null, null)
    }

    case class Previous_stays(
        var hotelName: String,
        var details: ListBuffer[Details]) {
      def this() = this(null, null)
    }

    case class Hotelname(
        var last_purchase_date: String,
        var length_of_last_stay: Double,
        var number_of_past_bookings: Double,
        var upcoming_stays: ListBuffer[Upcoming_stays],
        var previous_stays: ListBuffer[Previous_stays]) {
      def this() = this(null, 0, 0, null, null)
    }

    case class Customer(
        var _id: Long,
        var _rev: Long,
        var creation_date: String,
        var last_update: String,
        var hotelname: Hotelname) {
      def this() = this(0, 0, null, null, null)
    }

    var cust = new Customer
    var hotel = new Hotelname
    var upcoming_stays_list = new ListBuffer[Upcoming_stays]
    var details_list = new ListBuffer[Details]
    var upcoming_stays = new Upcoming_stays
    var details = new Details
    
   // upcoming_stays_list.toList

    val conf = new SparkConf().setAppName("DimensionAuditJob")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val customer_df = sqlContext.sql("SELECT * from test.customer")

    val customer_arr = customer_df.collect()

    for (c <- customer_arr) {
      
      val csId = c.getLong(0)

      val hConf = HBaseConfiguration.create()
      val myTable = new HTable(hConf, "customer")
      val g = new Get(Bytes.toBytes(csId));
      val result=myTable.get(g)
      if(!result.isEmpty()){
        
      }else {
      
      cust._id = c.getLong(0)
      cust._rev = c.getLong(1)
      cust.creation_date = Calendar.getInstance().getTime().toString()

      cust.hotelname = hotel
      upcoming_stays_list += upcoming_stays
      hotel.upcoming_stays = upcoming_stays_list

      upcoming_stays.hotel_name = c.getString(4)
      upcoming_stays.details = details_list

      details.length_of_stay = c.getString(6)
      details.arrival_date = c.getString(7)
      //details.booking_date = 
      details.departure_date = c.getString(8)
      details.reservation_status = c.getString(3)
      details.season = "Winter"
      //   details.upcoming_reservation_date = 
      details_list += details
      implicit val formats = Serialization.formats(NoTypeHints)
      val ser = org.json4s.native.Serialization.write(cust)
      println(ser)
     // val hConf = HBaseConfiguration.create()
      //val myTable = new HTable(hConf, "customer")
      var put = new Put(cust._id.toString().getBytes)

      put.add("cf".getBytes(), "cust_data".getBytes(), ser.getBytes());
      myTable.put(put);
      }
    }
  }
}

//spark-submit --class info.hadooptutorial.service.CustomerService --master local parseJson-0.0.1-SNAPSHOT-jar-with-dependencies.jar

