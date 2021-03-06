package com.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用本地文件创建RDD
 * @author alanwang
 *
 */
public class LocalFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("LocalFile")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/spark.txt");
		 
		JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>(){
			private static final long serialVersionUID = 1L;
			public Integer call(String v1) throws Exception{
				return v1.length();
			}
		});
		
		int count = lineLength.reduce(new Function2<Integer,Integer,Integer>(){
			private static final long serialVersionUID = 1L;
			public Integer call(Integer v1, Integer v2) throws Exception{
				return v1 + v2;
			}
		});
		
		System.out.println("总字数：" + count);
		
		sc.close();
	}
}
