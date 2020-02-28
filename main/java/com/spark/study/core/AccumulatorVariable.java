package com.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

/**
 * 累加变量
 * @author alanwang
 *
 */
public class AccumulatorVariable {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("AccumulatorVariable")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		final LongAccumulator sum = sc.sc().longAccumulator();
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		numbers.foreach(
				new VoidFunction<Integer>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Integer t) throws Exception {
						sum.add(t.longValue());
						
					}
				});
		System.out.println(sum.value());
		sc.close();
	}
}
