package com.jobreadyprogrammer.spark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
	
	public static void main(String args[]) {
		
		SparkSession spark = SparkSession.builder().appName("Name").master("local").getOrCreate();
		
		Dataset<Row> df = spark.read().format("text").load("src/main/resources/wordsList.txt");
		df.groupBy("value").count().show();

//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Name");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> textFile = sc.textFile("src/main/resources/wordsList.txt");
//        JavaPairRDD<String, Integer> counts = textFile
//        	    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//        	    .mapToPair(word -> new Tuple2<>(word, 1))
//        	    .reduceByKey((a, b) -> a + b);
//        	counts.take(10);
//        	System.out.println(counts.collect());
        

	}

}
