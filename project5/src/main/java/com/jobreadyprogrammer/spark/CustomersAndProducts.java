package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {

	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession.builder()
		        .appName("Learning Spark SQL Dataframe API")
		        .master("local")
		        .getOrCreate();
	    
		String customers_file = "src/main/resources/customers.csv";
		 
	    Dataset<Row> customersDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(customers_file);
	    
	    String products_file = "src/main/resources/products.csv";
		 
	    Dataset<Row> productsDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(products_file); 
	    
	    String purchases_file = "src/main/resources/purchases.csv";

		 
	    Dataset<Row> purchasesDf = spark.read().format("csv")
	        .option("inferSchema", "true") // Make sure to use string version of true
	        .option("header", true)
	        .load(purchases_file); 
	    
		System.out.println(" Loaded all files into Dataframes ");
    	System.out.println("----------------------------------");
    	
	    
	    Dataset<Row> joinedData = customersDf.join(purchasesDf, 
	    		customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
	    		.join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
	    		.drop("favorite_website").drop(purchasesDf.col("customer_id"))
	    		.drop(purchasesDf.col("product_id")).drop("product_id");
	    
	   Dataset<Row> aggDf = joinedData.groupBy("first_name", "product_name").agg(
	    		count("product_name").as("number_of_purchases"),
	    		max("product_price").as("most_exp_purchase"),
	    		sum("product_price").as("total_spent")
	    		);
	   
	   aggDf = aggDf.drop("number_of_purchases").drop("most_exp_purchase");
	    
	   Dataset<Row> initialDf = aggDf;
	   
	   for(int i = 0; i < 500; i++ ) {
		   aggDf = aggDf.union(initialDf);
	   }
	   
	   joinedData.collectAsList();
	    
	    
	}

}
