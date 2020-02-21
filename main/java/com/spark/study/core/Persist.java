package com.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 * @author alanwang
 *
 */
public class Persist {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("persist");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		System.out.println("Not using cache:");
		JavaRDD<String> lines1 = sc.textFile("hdfs://master:9000/spark.txt");
		long beginTime = System.currentTimeMillis();
		long count = lines1.count();
		System.out.println(count + " lines");
		long endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds.");
		
		beginTime = System.currentTimeMillis();
		count = lines1.count();
		System.out.println(count + " lines");
		endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds.");
		
		//必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
		//如果先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是无效的
		//而且会报错，导致大量文件丢失
		System.out.println("Using cache:");
		JavaRDD<String> lines2 = sc.textFile("hdfs://master:9000/spark.txt")
				.cache();
		beginTime = System.currentTimeMillis();
		count = lines2.count();
		System.out.println(count + " lines");
		endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds.");
		
		beginTime = System.currentTimeMillis();
		count = lines2.count();
		System.out.println(count + " lines");
		endTime = System.currentTimeMillis();
		System.out.println("cost " + (endTime - beginTime) + " milliseconds.");
		
		
		sc.close();
	}
}
