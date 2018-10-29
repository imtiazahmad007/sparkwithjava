package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearMarketingVsSales {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("LinearRegressionExample")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> markVsSalesDf = spark.read()
			.option("header", "true")
			.option("inferSchema", "true")
			.format("csv")
			.load("/Users/imtiazahmad/Desktop/SparkCourse/data/marketing_vs_sales.csv");

		Dataset<Row> mldf = markVsSalesDf.withColumnRenamed("sales", "label")
		.select("label", "marketing_spend","bad_day");
		
		String[] featureColumns = {"marketing_spend", "bad_day"};
		
		VectorAssembler assember = new VectorAssembler()
						.setInputCols(featureColumns)
						.setOutputCol("features");
		
		Dataset<Row> lblFeaturesDf = assember.transform(mldf).select("label", "features");
		lblFeaturesDf = lblFeaturesDf.na().drop();
		lblFeaturesDf.show();
		
		// next we need to create a linear regression model object
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel learningModel = lr.fit(lblFeaturesDf);
		
		learningModel.summary().predictions().show();
		
		System.out.println("R Squared: "+ learningModel.summary().r2());
		
		
		
	}
}
