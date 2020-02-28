package com.spark.study.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 基于transform的实时黑名单过滤
 * @author alanwang
 *
 */
public class TransformBlacklist {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("HDFSWordCount");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		// 用户对我们的网站上的广告可以进行点击
		// 点击之后，是不是要进行实时计费，点一下，算一次钱
		// 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
		// 只要是黑名单中的用户点击的广告，我们就给过滤掉
				
		// 先做一份模拟的黑名单RDD	
		List<Tuple2<String, Boolean>> blacklistData = new ArrayList<Tuple2<String,Boolean>>();
		blacklistData.add(new Tuple2<String, Boolean>("Tom", false));
		final JavaPairRDD<String, Boolean> blacklistRdd = jssc.sparkContext().parallelizePairs(blacklistData);
		
		//日志格式就是date username格式
		JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("localhost", 9999);
		
		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(log -> new Tuple2<>(log.split(" ")[1], log));
		
		//然后，执行transform操作，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作
		//实时进行黑名单过滤
		JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(
				userAdsClickLogRdd ->{
					JavaPairRDD<String, Tuple2<String,Optional<Boolean>>> joinedRdd = userAdsClickLogRdd.leftOuterJoin(blacklistRdd);
					JavaPairRDD<String, Tuple2<String,Optional<Boolean>>> filteredRdd = joinedRdd.filter(tuple -> {
						if (tuple._2._2.isPresent()) {
							return tuple._2._2.get();
						}
						return true;
					});
					return filteredRdd.map(tuple -> tuple._2._1);
				});
		
		validAdsClickLogDStream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
