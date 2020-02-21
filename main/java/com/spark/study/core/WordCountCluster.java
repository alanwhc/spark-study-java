package com.spark.study.core;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;


import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/*
 *  wordcount程序集群运行
 */
public class WordCountCluster {
	public static void main(String[] args) {
		//将setMaster删除，默认自己连接
		//针对的不是本地文件，修改为hadoop hdfs上真正存储的大数据文件
		
		SparkConf conf = new SparkConf()
				.setAppName("WordCountCluster");
	
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("hdfs://master:9000/passage.txt");
	
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<String> call(String line) throws Exception{
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		JavaPairRDD<String,Integer> pairs = words.mapToPair(
				new PairFunction<String,String,Integer>(){
					private static final long serialVersionUID = 1L;
					public Tuple2<String,Integer> call(String word) throws Exception{
						return new Tuple2<String,Integer>(word,1);
					}
		});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
			new Function2<Integer,Integer,Integer>(){
				private static final long serialVersionUID = 1L;
					
				public Integer call(Integer v1, Integer v2) throws Exception{
					return v1 + v2;
				}
		});
		
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>(){
			private static final long serialVersionUID = 1L;
 
			public void call(Tuple2<String, Integer> wordCount) throws Exception{
				System.out.println(wordCount._1 + " appears " + wordCount._2 + " times.");
			}
		});
		
		sc.close();
	}
}
