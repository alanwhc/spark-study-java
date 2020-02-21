package com.spark.study.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet数据源之使用编程方式加载数据
 * @author alanwang
 *
 */
public class ParquetLoadData {
	public static void main(String[] args) throws AnalysisException {
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrameProgrammatically")
				.setMaster("local");
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("RDD2DataFrameProgrammatically")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		
		//读取parquet文件，创建一个dataframe 
		Dataset<Row> userDf = sqlSession.read().parquet("/Users/alanwang/Documents/Work/textFile/users.parquet");
		
		//将DataFrame注册为临时表，查询需要的数据
		userDf.createTempView("users");
		Dataset<Row> usersNameDf = sqlSession.sql("select name from users");
		
		//对查询出的DataFrame进行transformation操作，处理数据，并打印出来
		List<String> usersNameList = usersNameDf.javaRDD().map(
			new Function<Row, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Row v1) throws Exception {
					return "Name: " + v1.getString(0);
				}
			}).collect();
		
		usersNameList.forEach(e->{
			System.out.println(e);
		});
		
		sqlSession.catalog().dropTempView("users");
		
	}
}
