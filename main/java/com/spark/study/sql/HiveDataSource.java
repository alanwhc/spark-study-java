package com.spark.study.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hive数据源
 * @author alanwang
 *
 */

public class HiveDataSource {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("HiveDataSource");
		//创建JavaSparkContext
		//JavaSparkContext sc = new JavaSparkContext(conf);
		//创建HiveSparkContext
		SparkSession hiveSession = SparkSession
				.builder()
				.config(conf)
				.appName("HiveDataSource")
				.enableHiveSupport()
				.getOrCreate();
		
		//1.使用sql()，可以执行Hive中能执行的HiveQL语句
		
		//判断是否存在students_info表，如果存在则删除
		hiveSession.sql("DROP TABLE IF EXISTS student_info");
		//判断students_info表是否不存在，如果不存在则重建
		hiveSession.sql("CREATE TABLE IF NOT EXISTS student_info (name String,age INT)");
		//将学生基本信息数据导入students_info表
		hiveSession.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/local/spark-study/resources/student_info.txt' "
				+ "INTO TABLE student_info");
		
		//同样的方法，将学生成绩数据导入students_scores表
		hiveSession.sql("DROP TABLE IF EXISTS student_scores");
		hiveSession.sql("CREATE TABLE IF NOT EXISTS student_scores (name String,score INT)");
		hiveSession.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
				+ "INTO TABLE student_scores");

		//执行SQL查询，关联两张表查询成绩大于80的学生
		Dataset<Row> goodStudentDf = hiveSession.sql("SELECT si.name,si.age,ss.score "
				+ "FROM student_info si "
				+ "JOIN student_scores ss ON si.name = ss.name "
				+ "WHERE ss.score>=80");
		
		//将DataFrame中的数据保存到good_student_info表中
		hiveSession.sql("DROP TABLE IF EXISTS good_student_info");
		goodStudentDf.write().saveAsTable("good_student_info");
		
		List<Row> goodStudentsList = hiveSession.table("good_student_info").collectAsList();
		
		goodStudentsList.forEach(e->{
			System.out.println(e);
		});
	}
}
