package com.spark.study.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * 每日UV统计
 * @author alanwang
 *
 */
public class DailyUV {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DailyUV")
				.setMaster("local");
		SparkSession sparkSession = SparkSession
				.builder()
				.config(conf)
				.appName("DailyUV")
				.enableHiveSupport()
				.getOrCreate();
		
		//模拟用户访问日志
		List<String> userAccessLog = Arrays.asList(
				"2020-02-17,1101",
		        "2020-02-17,1101",
		        "2020-02-17,1102",
		        "2020-02-17,1102",
		        "2020-02-17,1103",
		        "2020-02-18,1101",
		        "2020-02-18,1103",
		        "2020-02-18,1102",
		        "2020-02-18,1102");
		//并行化集合，创建普通RDD
		@SuppressWarnings("resource")
		JavaRDD<String> userAccessLogRdd = new JavaSparkContext(sparkSession.sparkContext()).parallelize(userAccessLog);
		//将普通RDD转换为类型为Row的RDD
		JavaRDD<Row> userAccessLogRowRdd = userAccessLogRdd.map(
			new Function<String, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String row) throws Exception {
					return RowFactory.create(
							row.split(",")[0],
							Integer.valueOf(row.split(",")[1]));
				}
			});
		List<StructField> structFields = Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("userid", DataTypes.IntegerType, true)
		);
		//创建Dataset
		Dataset<Row> userAccessLogDs = sparkSession.createDataFrame(userAccessLogRowRdd, DataTypes.createStructType(structFields));
		
		//使用内置函数
		userAccessLogDs.groupBy("date")
			.agg(countDistinct("userid"))
			.show();
	}
}
