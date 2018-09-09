package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.LineMapper;

import breeze.linalg.Options.Value;

public class WordCount {

	public void start() {
		
		SparkSession spark = SparkSession.builder()
		        .appName("CSV to dataframe to Dataset<House> and back")
		        .master("local")
		        .getOrCreate();
		
		String filename = "src/main/resources/shakespeare.txt";
		
		Dataset<Row> df = spark.read().format("text")
		        .load(filename);
		
		 df.show(5);
		 df.printSchema();
		 
		  Dataset<String> lineDS = df.flatMap(
			        new LineMapper(), Encoders.STRING());
		
		 
		  lineDS.printSchema();
		  lineDS.show(10, 200);
		  
		  String boringWords = "(   'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
		  		"      'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
		  		"      'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
		  		"      'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
		  		"      'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she')";
		  
		  Dataset<Row> df2 = lineDS.toDF();
		  df2 = df2.groupBy("value").count();
		  df2 = df2.filter("lower(value) NOT IN" + boringWords);
		  df2 = df2.orderBy(df2.col("count").desc());
		  
		  
		  df2.printSchema();
		  df2.show(100);
	}
	

}
