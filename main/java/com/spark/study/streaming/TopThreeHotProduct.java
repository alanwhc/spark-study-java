package com.spark.study.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;

/**
 * 与Spark SQL结合统计热门商品前3
 * @author alanwang
 *
 */
public class TopThreeHotProduct {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("TopThreeHotProduct");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(1));
		
		//日志格式：
		//leo iphone mobile_phone
		
		JavaReceiverInputDStream<String> productClickLogsDStream = jssc.socketTextStream("localhost", 9999);
		
		//做一次映射，将每个种类的每个商品，映射为(category_product,1)的格式
		//从而在后续使用ReduceByKeyAndWindow，统计出一个窗口内每个种类的每个商品的点击次数
		JavaPairDStream<String, Integer> pairDStream = productClickLogsDStream.mapToPair(
				item -> new Tuple2<>(item.split(" ")[2] + "_" + item.split(" ")[1], 1));
		
		JavaPairDStream<String, Integer> productClickCountDStream = pairDStream.reduceByKeyAndWindow(
				(v1, v2) -> v1 + v2, 
				Durations.seconds(60), 
				Durations.seconds(10));
		
		//针对60s内每个种类每个商品的点击次数，用foreachRDD
		productClickCountDStream.foreachRDD(
			//将该RDD转化为Row类型
			categoryProductCountRdd -> {
				JavaRDD<Row> categoryProductCountRowRdd = categoryProductCountRdd.map(
					categoryProductCount -> 
						RowFactory.create(
							categoryProductCount._1.split("_")[0],	//category
							categoryProductCount._1.split("_")[1],	//product
							categoryProductCount._2));	//count
			//然后执行DataFrame转换
			List<StructField> structFields = Arrays.asList(
					DataTypes.createStructField("category", DataTypes.StringType, true),
					DataTypes.createStructField("product", DataTypes.StringType, true),
					DataTypes.createStructField("count", DataTypes.IntegerType, true)
				);
			
			SparkSession sparkSession = SparkSession
					.builder()
					.config(conf)
					.enableHiveSupport()
					.getOrCreate();
			
			Dataset<Row> categoryProductCountDs = sparkSession.createDataFrame(categoryProductCountRowRdd, DataTypes.createStructType(structFields));
			
			//将60s内的每个种类每个商品的点击次数数据，注册一个临时表
			categoryProductCountDs.createTempView("product_click_log");
			
			//执行SQL语句，针对临时表统计出排名前3的商品
			Dataset<Row> topThreeCategoryProductDs = sparkSession.sql(""
					+ "SELECT category,product,count "
					+ "FROM ("
						+ "SELECT "
							+ "category,"
							+ "product,"
							+ "count,"
							+ "row_number() OVER (PARTITION BY category ORDER BY count DESC) rank "
						+ "FROM product_click_log"
					+ ") tmp "
					+ "WHERE rank <= 3");
			
			topThreeCategoryProductDs.show();
			sparkSession.catalog().dropTempView("product_click_log");
		});
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
