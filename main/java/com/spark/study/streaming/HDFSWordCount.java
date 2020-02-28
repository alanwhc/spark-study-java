package com.spark.study.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 基于HDFS的实时WordCount程序
 * @author alanwang
 *
 */
public class HDFSWordCount {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("HDFSWordCount");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//首先，使用JavaStreamingContext的fileStream()方法，针对HDFS创建输入数据流
		JavaDStream<String> lines = jssc.textFileStream("hdfs://master:9000/wordcount_dir");
		
		//执行相关算子操作
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((v1,v2) -> v1 + v2);
		
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
