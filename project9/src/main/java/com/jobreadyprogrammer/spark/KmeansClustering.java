package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KmeansClustering {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("kmeans Clustering")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> wholeSaleDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load("/Users/imtiazahmad/Desktop/SparkCourse/data/Wholesale customers data.csv");
		wholeSaleDf.show();
		Dataset<Row> featuresDf = wholeSaleDf.select("channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen");
		
		VectorAssembler assembler = new VectorAssembler();
		assembler = assembler.setInputCols(new String[] {"channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen"})
				.setOutputCol("features");
		
		Dataset<Row> trainingData = assembler.transform(featuresDf).select("features");
		
		KMeans kmeans = new KMeans().setK(10);
		
		KMeansModel model = kmeans.fit(trainingData);
		
		System.out.println(model.computeCost(trainingData));
		model.summary().predictions().show();
		
		
	}

}
