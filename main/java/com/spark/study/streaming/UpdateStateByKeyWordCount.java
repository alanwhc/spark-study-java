package com.spark.study.streaming;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import scala.Tuple2;

/**
 * 基于updateStateByKey算子实现缓存机制的WordCount程序
 * @author alanwang
 *
 */
public class UpdateStateByKeyWordCount {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setAppName("UpdateStateByKeyWordCount")
				.setMaster("local[2]");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//如果要使用UpdateStateByKeyWordCount算子，则必须设置一个checkpoint目录，开启checkpoint机制
		//这样的话才能把每个key对应的state除了在内存中有，checkpoint中也有备份
		
		//开启checkpoint机制，设置一个HDFS目录即可
		jssc.checkpoint("/usr/local/wordcount_checkpoint");
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
		
		//通过UpdateStateByKey算子，可以实现直接通过Spark维护一份每个单词的全局计数
		//返回两个参数，对于每个单词在每次计算时都会调用这个函数
		//第一个参数values，相当与是batch中key的新值，可能有多个，例如(hello,1),(world,1)，这个参数就是(1,1)
		//第二个参数state，代表这个key之前的状态，泛型类型自己指定
		JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
			(values, state) -> {
				Integer newValue = 0;
				if (state.isPresent()) {
					newValue = state.get();
				}
				for(Integer value : values) {
					newValue += value;
				}
				return Optional.of(newValue);
			}
		);
		
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
