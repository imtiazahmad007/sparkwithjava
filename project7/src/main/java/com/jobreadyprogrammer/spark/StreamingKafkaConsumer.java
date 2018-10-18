package com.jobreadyprogrammer.spark;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingKafkaConsumer {

	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession.builder()
		        .appName("StreamingKafkaConsumer")
		        .master("local")
		        .getOrCreate();

		// Kafka Consumer
		Dataset<Row> messagesDf = spark.readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "test")
		  .load()
		  .selectExpr("CAST(value AS STRING)"); // lines.selectExpr("CAST key AS STRING", "CAST value AS STRING") For key value
		
		// message.show() // <-- Can't do this when streaming!
		Dataset<String> words = messagesDf
				  .as(Encoders.STRING())
				  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		Dataset<Row> wordCounts = words.groupBy("value").count();
//
		
		
		StreamingQuery query = wordCounts.writeStream()
				.outputMode("complete")
				.format("console")
				.start();
//		
		query.awaitTermination();
		

	}

}
