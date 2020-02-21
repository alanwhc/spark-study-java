package com.spark.study.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.activation.MailcapCommandMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * action操作实战
 * @author alanwang
 *
 */
@SuppressWarnings("unused")
public class ActionOperation {
	/*
	 * reduce算子
	 */
	private static void reduce() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		//并行化集合
		JavaRDD<Integer> numbers = sc.parallelize(numList);
		
		//使用reduce算子对集合中数字进行累加
		int sum = numbers.reduce(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		//打印结果
		System.out.println(sum);
		
		//关闭JavaSparkContext
		sc.close();
	}
	
	/*
	 * collect算子
	 */
	private static void collect() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("collect")
			.setMaster("local");
				
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		//模拟集合
		List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
				
		//并行化集合		
		JavaRDD<Integer> numbers = sc.parallelize(numList);
		
		JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * 2;
			}
		});
		
		// 不使用foreach action操作，使用远程集群上遍历rdd中的元素
		// 而使用collect操作，将分布在远程集群的doubleNumbers Rdd取到本地
		//不推荐使用，远程集群速度较慢，且数据量大的话，容易造成内存溢出
		List<Integer> doubleNumberList = doubleNumbers.collect();
		for(Integer num : doubleNumberList) {
			System.out.println(num);
		}
		
		sc.close();
		
	}
	
	/*
	 * count算子
	 */
	private static void count() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("count")
			.setMaster("local");
				
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		//模拟集合
		List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
				
		//并行化集合		
		JavaRDD<Integer> numbers = sc.parallelize(numList);
		
		//对RDD执行count操作，统计它有多少个元素
		long count = numbers.count();
		
		System.out.println(count);
		
		sc.close();
		
	}
	/*
	 * take算子
	 */
	private static void take() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("take")
			.setMaster("local");
				
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		//模拟集合
		List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
				
		//并行化集合		
		JavaRDD<Integer> numbers = sc.parallelize(numList);
		
		//对RDD执行take操作，统计它有多少个元素
		//从远程服务器获取数据，take获取前n个数据
		List<Integer> topThreeNumber = numbers.take(3);
		
		topThreeNumber.forEach(e->{
			System.out.println(e);
		});
		
		
		sc.close();
		
	}
	
	/*
	 * saveAsTextFile算子
	 */
	private static void saveAsTextFile() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("saveAsTextFile")
			.setMaster("local");
				
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		//模拟集合
		List<Integer> numList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
				
		//并行化集合		
		JavaRDD<Integer> numbers = sc.parallelize(numList);
		
		//所有元素乘以2
		JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * 2;
			}
		});
		
		doubleNumbers.saveAsTextFile("hdfs://master:9000/double_number.txt");
		
		//关闭JavaSparkContext
		sc.close();
		
	}
	
	/*
	 * countByKey算子
	 */
	private static void countByKey() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
			.setAppName("countByKey")
			.setMaster("local");
				
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
				List<Tuple2<String, String>> studentList = Arrays.asList(
						new Tuple2<String, String>("class1","leo"),
						new Tuple2<String, String>("class2", "Jack"),
						new Tuple2<String, String>("class1", "mary"),
						new Tuple2<String, String>("class2", "david"),
						new Tuple2<String, String>("class2", "john"));
				
		//并行化集合		
		JavaPairRDD<String,String> students = sc.parallelizePairs(studentList);
		
		//对RDD应用countByKey操作，统计每个班级的学生人数，也就是统计每个key对应的元素个数
		//countByKey返回类型，是Map<String,Object>
		Map<String, Long> studentCounts = students.countByKey();
		
		for(Map.Entry<String, Long> studentCount: studentCounts.entrySet()) {
			System.out.println(studentCount.getKey()+": " + studentCount.getValue());
		}
		
		//关闭JavaSparkContext
		sc.close();
		
	}
	
	public static void main(String[] args) {
		// reduce();
		//collect();
		// count();
		//take();
		//saveAsTextFile();
		countByKey();
	}
}
