package com.jobreadyprogrammer.spark;

import java.beans.Encoder;
import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Application {

	public static void main(String[] args) {
		
			
		SparkSession spark = SparkSession.builder()
		        .appName("Learning Spark SQL Dataframe API")
		        .master("local") // <--- need to remove this line to run on a live cluster
		        .getOrCreate();
		
		  
//		 String redditFile = "s3n://your-bucket-name/Reddit_2011-large";
		 
		 String redditFile = "/file/on/your/computer/Reddit_2007-small"; // <- change your file location
		 
		    Dataset<Row> redditDf = spark.read().format("json")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .load(redditFile);
		    
		    redditDf = redditDf.select("body");
		    Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
		    		r -> Arrays.asList(r.toString().replace("\n", "").replace("\r", "").trim().toLowerCase()
		    				.split(" ")).iterator(),
		    		Encoders.STRING());
		
		    Dataset<Row> wordsDf = wordsDs.toDF();
		    
		    Dataset<Row> boringWordsDf = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();

//			wordsDf = wordsDf.except(boringWordsDf); // <-- This won't work because it removes duplicate words!!

		    wordsDf = wordsDf.join(boringWordsDf, wordsDf.col("value").equalTo(boringWordsDf.col("value")), "leftanti");
		   
		    wordsDf = wordsDf.groupBy("value").count();
		    wordsDf.orderBy(desc("count")).show();
		    
	}

	
	
}
