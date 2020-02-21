package com.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * row number开窗函数
 * @author alanwang
 *
 */
public class RowNumberWindowFunction {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("RowNumberWindowFunction");
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("RowNumberWindowFunction")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		
		sparkSession.sql("DROP TABLE IF EXISTS sales");
		sparkSession.sql("CREATE TABLE IF NOT EXISTS sales ("
				+ "product STRING,"
				+ "category STRING,"
				+ "revenue BIGINT)");
		sparkSession.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/local/spark-study/resources/sales.txt' "
				+ "INTO TABLE sales");
		
		//使用row_number()开窗函数
		//作用：每个分组的数据，按照其排序顺序，打行号的标记
		Dataset<Row> topThreeSalesDs = sparkSession.sql(""
				+ "SELECT product,category,revenue "
				+ "FROM ("
					+ "SELECT "
						+ "product,"
						+ "category,"
						+ "revenue,"
						+ "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
					+ "FROM sales"
				+ ") tmp_sales "
				+ "WHERE rank <= 3");
		
		//将每组前三的数据保存到表中
		sparkSession.sql("DROP TABLE IF EXISTS topthree_sales");
		
		topThreeSalesDs.write().saveAsTable("topthree_sales");
	}
}
