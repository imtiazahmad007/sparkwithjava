package com.jobreadyprogrammer.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class TupleAndPairRddApp {

	public static void main(String[] args) {
	
//		List<Integer> inputData = new ArrayList<>();
//		
//		inputData.add(10);
//		inputData.add(20);
//		inputData.add(142);
//		inputData.add(49);
//		inputData.add(25);
//		inputData.add(16);
		

		
		SparkConf conf = new SparkConf().setAppName("tupleExample").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
////
//		JavaRDD<Integer> inputNumbersRdd = sc.parallelize(inputData);
		
//		JavaRDD<IntegerWithRoot> twoColumnRdd = inputNumbersRdd.map(v -> new IntegerWithRoot(v));
//		twoColumnRdd.foreach(v -> System.out.println(v.number + ", " + v.squareRoot));
		
//		JavaRDD<Tuple3<Integer, Double, String>> squaredTupleRdd = inputNumbersRdd.map( 
//				v -> new Tuple3<>(v, Math.sqrt(v), "This is 3rd arg")
//				);
//		squaredTupleRdd.foreach(v -> System.out.println(v));
		
	
		List<String> inputData = new ArrayList<>();

		inputData.add("WARN: client stopped connection");
		inputData.add("FATAL: GET request failed");
		inputData.add("WARN: client stopped connection");
		inputData.add("ERROR: Incorrect URL");
		inputData.add("ERROR: POST request failed");
		inputData.add("FATAL: File does not exist");
		inputData.add("ERROR: File does not exist");
		
		JavaRDD<String> logRdd = sc.parallelize(inputData);
		
		JavaPairRDD<String, Long> pairRdd = logRdd.mapToPair(v -> {
			String [] columns = v.split(":");
			String logLevel = columns[0];
			String message = columns[1];
			
			return new Tuple2<String, Long>(logLevel, 1L);
		});
		
		JavaPairRDD<String, Long> logLevelCountsRdd = pairRdd.reduceByKey((v1, v2) -> v1+ v2);
		logLevelCountsRdd.foreach(v -> System.out.println(v._1 + ": " + v._2));
		
		
	}

}

class IntegerWithRoot{
	
	int number;
	double squareRoot;
	
	public IntegerWithRoot(int i) {
		this.number = i;
		this.squareRoot = Math.sqrt(i);
	}
}

