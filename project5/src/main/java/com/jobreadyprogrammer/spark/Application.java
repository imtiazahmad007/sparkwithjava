package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


public class Application {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
		        .appName("Learning More Spark SQL")
		        .master("local")
		        .getOrCreate();
		
		
		 String studentsFile = "src/main/resources/students.csv";
		 
		    Dataset<Row> studentDf = spark.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .load(studentsFile); 
		    
		 String gradeChartFile = "src/main/resources/grade_chart.csv";
		 
		    Dataset<Row> gradesDf = spark.read().format("csv")
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .load(gradeChartFile);
		    

		     // How to join tables
		    // Talk about how you can get rid of the df.col() and just use col()
		    // Talk about using just column names in strings in the select
		    // Talk about how you can also just use the col() function instead of df.col()
		    // start with removing df. Then go on to remove the col() as well to show the stripped down version
		    // Talk about how adding filter after the select limits what you can filter! Unlike SQL.
		    // Always have your selects at the end of your filtering
			    studentDf.join(gradesDf,  studentDf.col("GPA").equalTo((gradesDf.col("gpa"))))
//			    	.drop("gpa").drop("GPA")
			    	.select(studentDf.col("student_name"), 
			    			gradesDf.col("letter_grade"),
			    			studentDf.col("favorite_book_title"),
			    			studentDf.col("GPA")) // must have this for below filter to work
			    	.filter(col("GPA").between(2, 3.5)).show();
//			    	.filter(upper(col("letter_grade")).like("B")).show();
			    	
		    
	}

	
	
}
