package com.jobreadyprogrammer.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class ArrayToDataset {

	public void start() {
		SparkSession spark = new SparkSession.Builder()
				.appName("Array To Dataset<String>")
				.master("local")
				.getOrCreate();
		
		String [] stringList = new String[] {"Banana", "Glass", "Computer", "Car"};
		
		List<String> data = Arrays.asList(stringList);
		
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

		Dataset<Row> df = ds.toDF();
		
	}
	
}
