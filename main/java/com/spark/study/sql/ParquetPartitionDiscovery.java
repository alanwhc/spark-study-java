package com.spark.study.sql;

/**
 * Parquet数据源之自动推断分区
 */

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetPartitionDiscovery {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ParquetPartitionDiscovery");
		
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("ParquetPartitionDiscovery")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		Dataset<Row> userDf = sqlSession.read().parquet("hdfs://master:9000/spark-study/users/gender=male/country=US/users.parquet");
		userDf.printSchema();
		userDf.show();
	}
}
