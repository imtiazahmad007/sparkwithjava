package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.LineMapper;

public class WordCount {

	public void start() {
		
		 String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
			  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
			  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
			  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
			  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
			  		"'your', 'you', 'I', "
			  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
		 
		SparkSession spark = SparkSession.builder()
		        .appName("unstructured text to flatmap")
		        .master("local")
		        .getOrCreate();
		
		String filename = "src/main/resources/shakespeare.txt";
		
		Dataset<Row> df = spark.read().format("text")
		        .load(filename);
		
//		df.printSchema();
//		df.show(10);
		
		Dataset<String> wordsDS = df.flatMap(new LineMapper(), Encoders.STRING());

		Dataset<Row> df2 = wordsDS.toDF();
		
		df2 = df2.groupBy("value").count();
		df2 = df2.orderBy(df2.col("count").desc());
		df2 = df2.filter("lower(value) NOT IN " + boringWords);
		
		df2.show(500);
		
		  
	}
	

}
