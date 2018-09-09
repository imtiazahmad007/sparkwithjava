package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.HouseMapper;
import com.jobreadyprogrammer.pojos.House;

public class CsvToDatasetHouseToDataframe {
	
	public void start() {
		
		SparkSession spark = SparkSession.builder()
		        .appName("CSV to dataframe to Dataset<House> and back")
		        .master("local")
		        .getOrCreate();
		
		
		 String filename = "src/main/resources/houses.csv";
		 
		    Dataset<Row> df = spark.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .option("sep", ";")
		        .load(filename);
		 
		    System.out.println("*** Houses ingested in a dataframe");
		    
		    df.show(5);
		    df.printSchema();
		 
		    
		    Dataset<House> houseDS = df.map(
		        new HouseMapper(), Encoders.bean(House.class));
		    
		    System.out.println("below schema is for house dataset");
		    houseDS.show(5, 17);
		    houseDS.printSchema();
		    
		    System.out.println("below is the houses dataframe");
		    
		    Dataset<Row> df2 = houseDS.toDF();

//		    df2.printSchema();
//		    df2.show(5, 17);
		    
		
	}
	

	    
}

 
   