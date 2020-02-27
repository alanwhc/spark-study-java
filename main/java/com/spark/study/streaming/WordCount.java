package com.spark.study.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 实时WordCount程序
 * @author alanwang
 *
 */
public class WordCount {
	public static void main(String[] args) throws Exception {
		//创建SparkConf对象，设置Master属性本地测试为local，要增加一个方括号
		//括号中填写数字，代表执行Spark Streaming时使用的线程数
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("WordCount");
		
		//创建Java Streaming Context对象，类似JavaSparkContext
		//除了接收SparkConf对象外，还需接收batch interval对象，即每隔多久划分一个batch
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(1));
		
		//首先创建输入DStream，代表了从Kafka、Socket中不断传入的实时数据流
		//调用socketTextStream方法，可以创建一个数据源为Socket网路端口的数据流
		//socketTextStream调取两个参数，主机名、端口号
		//底层RDD泛型类型与JavaReceiverInputDStream泛型类型一致，此处为String
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		//对底层RDD使用算子进行操作
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((i1,i2) -> i1+ i2);
		
		//打印这一秒单词计数情况，并休眠5秒便于观察结果		
		wordCounts.print();
		
		//对JavaStreamingContext进行后续处理
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}