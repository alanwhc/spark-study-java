package com.spark.study.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 以编程方式动态指定元数据，将RDD转化为DataFrame
 * @author alanwang
 *
 */
public class RDD2DataFrameProgrammatically {
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
		JavaSparkContext sc = new JavaSparkContext(sqlSession.sparkContext());
		
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/textFile/students.txt");
		
		//必须转换成RDD<Row>的格式
		JavaRDD<Row> studentRdd = lines.map(
			new Function<String, Row>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String line) throws Exception {
					String[] lineSplited = line.split(",");
					return RowFactory.create(
							Integer.valueOf(lineSplited[0]),
							lineSplited[1],
							Integer.valueOf(lineSplited[2]));
				}
			});
		
		//动态构造元数据
		//比如说，id、name等，field的名称和类型，都是不固定的，适合此方式构建元数据
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(structFields);
		
		//使用动态构造的元数据，将RDD转换为DataFrame
		Dataset<Row> studentDF = sqlSession.createDataFrame(studentRdd, structType);
		
		studentDF.createTempView("students");
		
		Dataset<Row> enquiryDf = sqlSession.sql("select * from students where age >= 18");
		
		//将DataFrame转为RDD，并collect
		List<Row> enquiryRdd = enquiryDf.javaRDD().collect();
		
		enquiryRdd.forEach(e->{
			System.out.println(e);
		});
		
		sqlSession.catalog().dropTempView("students");

		sc.close();
	}
}
