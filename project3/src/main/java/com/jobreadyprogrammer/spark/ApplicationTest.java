package com.jobreadyprogrammer.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ApplicationTest {

	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession.builder()
		        .appName("Learning Spark SQL Dataframe API")
		        .master("local")
		        .getOrCreate();
		
		String [] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Banana", "Computer", "Car", "IS", "HE"};
		
		List<String> words = Arrays.asList(stringList);
		
		Dataset<Row> wordsDf =  spark.createDataset(words, Encoders.STRING()).toDF();
		
		String [] bordingWords = new String[] {"this", "is", "he"};
		String filter = "( 'this', 'is', 'he')";
		List<String> bordingList = Arrays.asList(bordingWords);
		Dataset<Row> boringWordsDf =  spark.createDataset(bordingList, Encoders.STRING()).toDF();
		
		wordsDf = wordsDf.filter("value not in "+ filter);
		
		wordsDf.show();

		
	}

}
