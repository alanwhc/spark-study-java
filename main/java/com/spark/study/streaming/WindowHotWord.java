package com.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 基于Window的热点搜索词实时统计
 * @author alanwang
 *
 */
public class WindowHotWord {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("HDFSWordCount");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));	
		
		JavaReceiverInputDStream<String> searchLogDsDStream = jssc.socketTextStream("localhost", 9999);
		
		//将搜索日志转换成仅有搜索词的格式
		JavaDStream<String> searchWorDStream = searchLogDsDStream.map(searchLog -> searchLog.split(" ")[1]);
		//将搜索词映射成(word,1)格式
		JavaPairDStream<String, Integer> searchWordsPairDStream = searchWorDStream.mapToPair(searchWord -> new Tuple2<>(searchWord, 1));
		//针对searchWordsPairDStream执行reduceByKeyAndWindow操作
		//每隔10s会将前60s的数据聚合起来，也就是60 / 5 = 12个RDD聚合起来，统一执行reduceByKey操作
		JavaPairDStream<String, Integer> searchWordsCountDStream = searchWordsPairDStream.reduceByKeyAndWindow(
				(v1,v2) -> v1 + v2, 	//reduce函数
				Durations.seconds(60),	//窗口长度60s 
				Durations.seconds(10)	//滑动间隔10s
		);	
		
		//执行transform操作
		JavaDStream<Tuple2<String, Integer>> hotSearchWorDStream = searchWordsCountDStream.transform(
			searchWordCountRdd -> {
				//执行搜索词和出现频率的反转成(count,word)
				JavaPairRDD<Integer, String> countSearchWordsRdd = searchWordCountRdd.mapToPair(searchWordCount -> new Tuple2<>(searchWordCount._2, searchWordCount._1));
				//对反转后的JavaPairRDD进行降序排序
				JavaPairRDD<Integer, String> sortedSearchWordsRdd = countSearchWordsRdd.sortByKey(false);
				//然后再次进行反转成(word,count)
				JavaPairRDD<String, Integer> hotSearchWordsRdd = sortedSearchWordsRdd.mapToPair(sortedSearchWords -> new Tuple2<>(sortedSearchWords._2, sortedSearchWords._1));
				//取出热点词前3，并进行打印
				hotSearchWordsRdd.take(3).forEach(e -> System.out.println(e._1 + " appears " + e._2 + " times."));
				
				return searchWordCountRdd.map(word -> new Tuple2<>(word._1,word._2));
			}
		);
		
		hotSearchWorDStream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
