package com.jobreadyprogrammer.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class StreamingSocketApplication {

	public static void main(String[] args) throws StreamingQueryException {
		
			
		// First start a socket connection at 9999 using this: nc -lk 9999
		SparkSession spark = SparkSession.builder()
		        .appName("StreamingSocketWordCount")
		        .master("local")
		        .getOrCreate();
		
		// Create DataFrame representing the stream of input lines from connection to localhost:9999
		
		Dataset<Row> lines = spark
		  .readStream()
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 9999)
		  .load();
		
		Dataset<String> words = lines
				  .as(Encoders.STRING())
				  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
		
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		StreamingQuery query = wordCounts.writeStream()
				.outputMode("append")
				.format("console")
				.start();
		
		query.awaitTermination();
		
		
		
		
		    
	}

	
	
}
