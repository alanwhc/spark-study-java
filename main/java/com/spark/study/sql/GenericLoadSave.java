package com.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 通用load与save操作
 * @author alanwang
 *
 */

public class GenericLoadSave {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("GenericLoadSave")
				.setMaster("local");
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("GenericLoadSave")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sqlSession.sparkContext());
		
		Dataset<Row> usersDf = sqlSession.read().load("/Users/alanwang/Documents/Work/textFile/users.parquet");
		usersDf.select("name","favorite_color").write().save("/Users/alanwang/Documents/Work/textFile/target.parquet");
		
		
		sc.close();
	}
}
