package com.spark.study.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import static org.apache.spark.sql.functions.*;

/**
 * 每日销售额统计
 * @author alanwang
 *
 */
public class DailySales {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DailySales")
				.setMaster("local");
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("DailySales")
				.config(conf)
				.getOrCreate();
		
		//模拟每日销售日志
		List<String> salesLog = Arrays.asList(
				"2020-02-17,100.04,1101",
		        "2020-02-17,78.11,1101",
		        "2020-02-17,67.98,1102",
		        "2020-02-17,200.29",//用户id丢失
		        "2020-02-17,87.29,1103",
		        "2020-02-18,1000.23",//用户id丢失
		        "2020-02-18,789.21,1103",
		        "2020-02-18,123.21,1102",
		        "2020-02-18,28.12,1102");
		//并行化集合，创建JavaRDD
		JavaRDD<String> salesLogRdd = new JavaSparkContext(sparkSession.sparkContext()).parallelize(salesLog);
	
		//过滤掉用户id为空的日志，仅统计有效销售额
		JavaRDD<String> filteredSalesLofRdd = salesLogRdd.filter(
			new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(String log) throws Exception {
					return log.split(",").length == 3?true:false;
				}
			});
		
		//将普通RDD转换为类型为Row的RDD
		JavaRDD<Row> salesLogRowRdd = filteredSalesLofRdd.map(
			new Function<String, Row>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String log) throws Exception {
					return RowFactory.create(log.split(",")[0],
							Double.valueOf(log.split(",")[1]));
				}
			});
		
		//构建Struct field
		List<StructField> structFields = Arrays.asList(
			DataTypes.createStructField("date", DataTypes.StringType, true),
			DataTypes.createStructField("sales", DataTypes.DoubleType, true));
		
		//将RDD转化为DataFrame
		Dataset<Row> salesLogRowDs = sparkSession.createDataFrame(salesLogRowRdd, DataTypes.createStructType(structFields));
		
		//对DataFrame进行内置sum函数操作，并展示结果
		salesLogRowDs.groupBy("date")
			.agg(sum("sales"))
			.show();
	}
}
