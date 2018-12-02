package info.yahoo.finance

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.HashMap
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import yahoofinance._

object YahooLogProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    //"INTC"
    var symbols = Array("INTC","BABA", "TSLA", "AIR.PA", "YHOO")
    // Send some messages
    while (true) {
      var stocks = YahooFinance.get(symbols, true)
      var keySet = stocks.keySet()
      var i=0;
      for (e <- 0 until (keySet.size())) {
        //    stocks.keySet().forEach ((stockName: String) =>
        val stock = stocks.get(symbols.apply(i))
        i=i+1;
            if (stock != null) {

          var stockCurrency = ""
          if (stock.getCurrency != null) {
            stockCurrency = stock.getCurrency
          }

         /* var stockAnnualDuvidend = stock.getDividend.getAnnualYield;
          var stockQuotePrice = stock.getQuote.getPrice;
          var stockPeg = stock.getStats.getPeg*/

          //var stockValue = stock.getName + "|" + stockCurrency + "|" + stockCurrency + "|" + stockAnnualDuvidend + "|" + stockQuotePrice + "|" + stockPeg;
          var stocksymbol=stock.getSymbol
          var sName=stock.getName
          var stockQuotePrice = stock.getQuote.getPrice
          var stockexge=stock.getStockExchange
          var stockVolume=stock.getQuote.getVolume
           var stockopen=stock.getQuote.getOpen
          var stockValue=stocksymbol + "|" + sName + "|" + stockQuotePrice + "|" + stockexge + "|" + stockCurrency + "|" + stockVolume + "|" + stockopen;
          stockValue = stockValue.mkString("")
          
          val message = new ProducerRecord[String, String](topic, stockValue)

          println(message + "======message")
          producer.send(message)
        }
        };

      Thread.sleep(1000)
    }
  }

}
