package com.jobreadyprogrammer.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapAndReduceRddApp {

	public static void main(String[] args) {

		List<Double> inputData = new ArrayList<>();
		
		inputData.add(9.00);
		inputData.add(4.00);
		inputData.add(83.00);
		inputData.add(142.00);
		inputData.add(75.00);
		inputData.add(25.00);
		
		SparkConf conf = new SparkConf().setAppName("RddMapReduce").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Double> myRdd = sc.parallelize(inputData);
		
		// Map function:
		JavaRDD<Double> squareRootRdd = myRdd.map(v -> Math.sqrt(v));
		squareRootRdd.foreach(v -> System.out.println(v)); // System.out::println
		
		// count number of elements using map and reduce functions
		JavaRDD<Integer> counterRdd = squareRootRdd.map(v1 -> 1);
		int count = counterRdd.reduce((v1, v2) -> v1 + v2);
		System.out.println(count);
		
	}

}
