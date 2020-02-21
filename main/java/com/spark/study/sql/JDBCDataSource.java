package com.spark.study.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * JDBC数据源
 * @author alanwang
 *
 */
public class JDBCDataSource {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("JDBCDataSource");
		//创建SparkSession
		SparkSession sqlSession = SparkSession
			.builder()
			.config(conf)
			.appName("JDBCDataSource")
			.enableHiveSupport()
			.getOrCreate();
		
		//将两张表的数据加载为DataSet
		Map<String, String> options = new HashMap<String,String>();
		options.put("url", "jdbc:mysql://master:3306/testdb");
		options.put("dbtable", "student_info");
		Dataset<Row> studentInfoDs = sqlSession.read().format("jdbc").options(options).load();
		
		options.put("dbtable", "student_scores");
		Dataset<Row> studentScoreDs = sqlSession.read().format("jdbc").options(options).load();
		
		//将两个DataFrame转换为JavaPairRDD，执行join操作
		JavaPairRDD<String, Tuple2<Integer, Integer>> studentRdd = 
				studentInfoDs.javaRDD().mapToPair(
					new PairFunction<Row, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Row t) throws Exception {
						return new Tuple2<String, Integer>(String.valueOf(t.get(0)), 
							Integer.valueOf(String.valueOf(t.get(1))));
					}
				})
			.join(studentScoreDs.javaRDD().mapToPair(
				new PairFunction<Row, String, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> call(Row t) throws Exception {
						return new Tuple2<String, Integer>(String.valueOf(t.get(0)), 
							Integer.valueOf(String.valueOf(t.get(1))));
					}
				}));
		//将JavaPairRDD转换为JavaRDD<Row>
		JavaRDD<Row> studentsRowsRdd = studentRdd.map(
				new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
						return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
					}
				});
		
		//过滤数据
		JavaRDD<Row> filteredStudentRowsRdd = studentsRowsRdd.filter(
				new Function<Row, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Row row) throws Exception {
						return row.getInt(2)>80?true:false;
					}
				});
		
		//转换为DataFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);

		Dataset<Row> studentsDs = sqlSession.createDataFrame(filteredStudentRowsRdd, structType);
		
		//打印结果
		List<Row> rows = studentsDs.collectAsList();
		rows.forEach(e->{
			System.out.println(e);
		});
		
		//将DataSet中的数据保存到MySQL表中
		studentsDs.javaRDD().foreach(
				new VoidFunction<Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Row row) throws Exception {
						String sql = "insert into good_student_info values ("
								+ "'" + String.valueOf(row.get(0)) + "',"
								+ Integer.valueOf(String.valueOf(row.get(1))) + ","
								+ Integer.valueOf(String.valueOf(row.get(2))) + ")";
						Class.forName("com.mysql.jdbc.Driver");
						Connection conn = null;
						Statement stmt = null;
						try {
							conn = DriverManager.getConnection(
									"jdbc:mysql://master:3306/testdb","","");
							stmt = conn.createStatement();
							stmt.executeUpdate(sql);
						} catch (Exception e2) {
							e2.printStackTrace();
						} finally {
							if(stmt != null) {
								stmt.close();
							}
							if(conn != null) {
								conn.close();
							}
						}
					}
				});
	}
}
