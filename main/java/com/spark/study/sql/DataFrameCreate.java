package com.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * 使用json文件创建DataFrame
 * @author alanwang
 *
 */
public class DataFrameCreate {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate")
				.setMaster("local");
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("dataframe")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sqlSession.sparkContext());
		Dataset<Row> df = sqlSession.read().json("hdfs://master:9000/students.json");
		//打印DataFrame中所有的数据——select * from ...
		df.show();
		
		sc.close();
	}
}
