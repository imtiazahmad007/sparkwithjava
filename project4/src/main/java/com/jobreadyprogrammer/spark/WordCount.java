package com.jobreadyprogrammer.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.pojos.Line;

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
		 
		 Dataset<Line> houseDS = df.map(
			        new MapFunction<Row, Line>(){
			        	
			        	private static final long serialVersionUID = -2L;
			        	
						@Override
						public Line call(Row value) throws Exception {
							String[] words = value.toString().split(" ");
							Line l = new Line();
							l.setWords(words);
							
							return l;
						}
			        	
			        },
			        
			        Encoders.bean(Line.class));
		 
		 houseDS.printSchema();
		 houseDS.show(10, 50);
	}
	

}
