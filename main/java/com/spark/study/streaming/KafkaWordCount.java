package com.spark.study.streaming;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;


/**
 * 基于Kafka的wordcount程序
 * @author alanwang
 *
 */

public class KafkaWordCount {
	public static void main(String[] args) throws InterruptedException {	
		
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("HDFSWordCount");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		String brokers = "172.19.205.205:9092,47.103.113.98:9092,101.133.157.138:9092";	//监听端口
		String groupId = "consumer-group";	//消费者id
		String topics = "wordcount";	//会话
		
		//会话初始化
		Set<String> topicSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		
		//Kafka参数初始化 
		Map<String, Object> kafkaParams = new HashMap<String, Object>();

		//Kafka监听端口
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		//消费者id
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		//指定Kafka输出Key的数据类型及编码格式
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		//指定Kafka输出valu的数据类型及编码格式
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		//指定从latest还是smallest出开始读取数据
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		//定期往ZooKeeper写入每个分区的offset
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		final JavaInputDStream<ConsumerRecord<String, String>> msgs = 
				KafkaUtils.createDirectStream(
						jssc, 
						LocationStrategies.PreferConsistent(), 
						ConsumerStrategies.Subscribe(topicSet, kafkaParams));
		
		JavaDStream<String> lines = msgs.map(ConsumerRecord::value);
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((v1,v2) -> v1 + v2);
		
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
