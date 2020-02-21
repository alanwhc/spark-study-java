package com.spark.study.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 取最大的前三个数字
 * @author alanwang
 *
 */
public class TopThree {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("TopThree")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/textFile/top.txt");
		JavaPairRDD<Integer,String> pairs = lines.mapToPair(
				new PairFunction<String, Integer, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<Integer, String> call(String t) throws Exception {
						return new Tuple2<Integer, String>(Integer.valueOf(t), t);
					}
				});
		
		JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);
		
		JavaRDD<Integer> sortedNumbers = sortedPairs.map(
			new Function<Tuple2<Integer,String>, Integer>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Integer call(Tuple2<Integer, String> v1) throws Exception {
					return v1._1;
				}
			});
		
		List<Integer> sortedNumberList = sortedNumbers.take(3);
		
		sortedNumberList.forEach(e->{
			System.out.println(e);
		});
		
		sc.close();
	}
}