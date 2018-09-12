package com.jobreadyprogrammer.mappers;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

public class LineMapper implements FlatMapFunction<Row, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<String> call(Row row) throws Exception {
		return Arrays.asList(row.toString().split(" ")).iterator();
	}
	
}
