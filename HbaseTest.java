package com.java.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {

	public static void main(String[] args) throws IOException {
		 Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class
	      HTable table = new HTable(config, "emp");

	      // Instantiating Get class
	      Get g = new Get(Bytes.toBytes("row1"));

	      // Reading the data
	      Result result = table.get(g);

	}
}
