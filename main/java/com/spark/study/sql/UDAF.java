package com.spark.study.sql;

import java.util.Arrays;
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

/**
 * User Defined Aggregate Function
 * @author alanwang
 *
 */

public class UDAF {

	public static void main(String[] args) throws AnalysisException {
		SparkConf conf = new SparkConf()
				.setAppName("UDAF")
				.setMaster("local");
		
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("UDAF")
				.config(conf)
				.getOrCreate();
		List<String> names = Arrays.asList("Leo","Marry","Tom","Jack","Leo","Jack","Jack","Tom","Tom","Tom");
		@SuppressWarnings("resource")
		JavaRDD<String> namesRdd = new JavaSparkContext(sparkSession.sparkContext()).parallelize(names);
		JavaRDD<Row> namesRowRdd = namesRdd.map(
			new Function<String, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String row) throws Exception {
					return RowFactory.create(row);
				}
			});
		List<StructField> structFields = Arrays.asList(
				DataTypes.createStructField("name", DataTypes.StringType, true));
		Dataset<Row> namesDs = sparkSession.createDataFrame(namesRowRdd,DataTypes.createStructType(structFields));
		
		namesDs.createTempView("names");
		
		sparkSession.udf().register("strCount", new StringCount());
		
		sparkSession.sql("select name, strCount(name) from names group by name")
			.collectAsList()
			.forEach(e->{
				System.out.println(e);
			});
		
		sparkSession.catalog().dropTempView("names");
	}
}
