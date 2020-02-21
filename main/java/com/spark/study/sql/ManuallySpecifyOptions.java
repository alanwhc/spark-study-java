package com.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 手动指定数据源类型
 * @author alanwang
 *
 */
public class ManuallySpecifyOptions {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ManuallySpecifyOptions")
				.setMaster("local");
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("ManuallySpecifyOptions")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sqlSession.sparkContext());
		
		Dataset<Row> peopleDf = sqlSession.read().format("json").load("/Users/alanwang/Documents/Work/textFile/people.json");
		peopleDf.select("name").write().format("parquet").save("/Users/alanwang/Documents/Work/textFile/peopleName.parquet");
		
		sc.close();
	}
}
