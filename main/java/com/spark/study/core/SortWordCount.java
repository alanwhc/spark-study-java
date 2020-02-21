package com.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SortWordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SortWordCount")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//创建lines RDD
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/spark.txt");
		
		//执行单词计数操作，获得每个单词出现的次数
		JavaRDD<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Iterator<String> call(String t) throws Exception {
						return Arrays.asList(t.split(" ")).iterator();
					}
				});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						return new Tuple2<String, Integer>(t, 1);
					}
					
				});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		/*按照每个单词出现的次数降序排序
		*wordCounts RDD内的元素形式为(word1,1),(word2,2)...
		*需要将RDD转换成(1,word1),(2,word2)的形式，才可以排序
		*/
		
		//进行key-value的反转映射
		JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(
				new PairFunction<Tuple2<String,Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				});
		
		//按照key进行排序
		JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);
		
		//进行key-value反转映射
		JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(
				new PairFunction<Tuple2<Integer,String>, String, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
						return new Tuple2<String, Integer>(t._2, t._1);
					}
				});
		
		sortedWordCounts.foreach(
				new VoidFunction<Tuple2<String,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> t) throws Exception {
						System.out.println(t._1 + " appears " + t._2 + " times.");
						
					}
				});
		
		//关闭SparkContext
		sc.close();
	}
}
