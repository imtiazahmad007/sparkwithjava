package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
		        .appName("Learning More Spark SQL")
		        .master("local")
		        .getOrCreate();
		
		
		 String filename = "src/main/resources/grade_chart.csv";
		 
		    Dataset<Row> df = spark.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .load(filename);
		    
		    df.show(10);
		
	}

	
}
