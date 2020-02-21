package com.spark.study.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RDD2DataFrameReflection {
	public static void main(String[] args) throws AnalysisException {
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrameReflection")
				.setMaster("local");
		SparkSession sqlSession = SparkSession
				.builder()
				.appName("RDD2DataFrameReflection")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sqlSession.sparkContext());
		
		//创建普通RDD	
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/textFile/students.txt");
		JavaRDD<Student> students = lines.map(
			new Function<String, Student>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Student call(String line) throws Exception {
					String[] lineSplited = line.split(",");
					Student stu = new Student();
					stu.setId(Integer.valueOf(lineSplited[0].trim()));
					stu.setName(lineSplited[1]);
					stu.setAge(Integer.valueOf(lineSplited[2].trim()));
					return stu;
				}
				
			});
		
		//使用反射方式，将RDD转化为DataFrame
		//将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		//因为Student.class本身就是反射的一个应用
		//然后底层要通过对Student class进行反射，来获取其中的field
		Dataset<Row> studentDF = sqlSession.createDataFrame(students, Student.class);
		
		//拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
		studentDF.createTempView("students");
		
		//针对临时表执行SQL语句，查询年龄小于等于18岁的学生
		Dataset<Row> teenagerDF = sqlSession.sql("select * from students where age >= 18");
		
		//将查询出来的DataFrame，再次转换为RDD
		JavaRDD<Row> teenagerRdd = teenagerDF.javaRDD();
		
		//将RDD中的数据，进行映射，映射为Student
		JavaRDD<Student> teenagerStudentRdd = teenagerRdd.map(
			new Function<Row, Student>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Student call(Row row) throws Exception {
					Student stu = new Student();
					stu.setAge(row.getInt(0));
					stu.setId(row.getInt(1));
					stu.setName(row.getString(2));
					return stu;
				}
			});
		
		//将数据collect，并打印
		List<Student> studentList = teenagerStudentRdd.collect();
		studentList.forEach(e->{
			System.out.println(e);
		});
		
		sqlSession.catalog().dropTempView("students");
		sc.close();
	}
}
