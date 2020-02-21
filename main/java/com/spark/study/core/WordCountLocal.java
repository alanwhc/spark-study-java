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
 * Java开发本地测试的wordcount程序
 */
public class WordCountLocal {
	public static void main(String[] args) {
		//Step 1：sparkconf对象
		//setMaster()可以设置Spark应用程序要连接的master节点的url
		//local表示本地运行
		SparkConf conf = new SparkConf()
				.setAppName("WordCountLoca")
				.setMaster("local");
		
		//Step2：创建JavaSparkContext对象
		//在Spark中，SparkContext是Spark所有功能的入口，主要作用是初始化Spark的核心组件
		//包括调度器(DAGSchedule,TaskScheduler），还会去Spark Master节点上注册
		//Java -> JavaSparkContext
		//Spark SQL -> SQLContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Step3：针对输入源（HDFS，本地文件等），创建一个初始的RDD
		//在Java中，创建的普通RDD，都叫做JavaRDD
		//如果是HDFS或者文件，创建的RDD中每一个元素都是文件中的一行
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Desktop/passage.txt");
		
		//Step4：对初始RDD进行transformation操作，即计算操作
	
		//先将每一行拆分成单词
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<String> call(String line) throws Exception{
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		//接着，需要将每一个单词，映射为（单词,1）的格式
		JavaPairRDD<String,Integer> pairs = words.mapToPair(
				new PairFunction<String,String,Integer>(){
					private static final long serialVersionUID = 1L;
					public Tuple2<String,Integer> call(String word) throws Exception{
						return new Tuple2<String,Integer>(word,1);
					}
		});
		
		//接着，需要以单词作为key，统计每个单词出现的次数
		//使用reduceByKey算子，对每个key对应的value，进行reduce操作
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
			new Function2<Integer,Integer,Integer>(){
				private static final long serialVersionUID = 1L;
					
				public Integer call(Integer v1, Integer v2) throws Exception{
					return v1 + v2;
				}
		});
		
		//
		//最后，使用action操作。一个Spark应用程序只有transformation是不行的，必须有action操作触发程序
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>(){
			private static final long serialVersionUID = 1L;
 
			public void call(Tuple2<String, Integer> wordCount) throws Exception{
				System.out.println(wordCount._1 + " appears " + wordCount._2 + " times.");
			}
		});
		
		sc.close();
	}
}
