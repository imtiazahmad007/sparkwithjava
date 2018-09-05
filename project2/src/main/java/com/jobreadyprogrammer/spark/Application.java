package com.jobreadyprogrammer.spark;

public class Application {

	public static void main(String[] args) {
		
//		InferCSVSchema parser = new InferCSVSchema();
//		parser.printSchema();
//		
//		DefineCSVSchema parser2 = new DefineCSVSchema();
//		parser2.printDefinedSchema();
		
		JSONLinesParser parser = new JSONLinesParser();
		parser.parseJsonLines();

	}

}
