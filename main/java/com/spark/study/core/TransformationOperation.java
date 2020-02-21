package com.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * transformation操作实战
 * @author alanwang
 *
 */
@SuppressWarnings("unused")
public class TransformationOperation {
	/*
	 * map算子：将集合中每个元素都乘以2
	 */
	private static void map() {
		SparkConf conf = new SparkConf()
				.setAppName("map")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		 
		//并行化集合，创建初始RDD
		JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
		
		//使用map算子，将集合中的每个元素都乘以2
		//map算子，是对任何RDD，都可以调用的
		//在Java中，map算子接收的参数是Function对象
		JavaRDD<Integer> doubleNumbRdd = numberRdd.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer t) throws Exception {
				return t *2;
			}
		});
		
		doubleNumbRdd.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();
	}
	
	/*
	 * filter算子：过滤出集合中的偶数
	 */
	private static void filter() {
		SparkConf conf = new SparkConf()
				.setAppName("filter")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		JavaRDD<Integer> numJavaRDD = sc.parallelize(numbers);
		
		//如果想在新的RDD中保留这个元素，那么就返回true；否则返回false
		JavaRDD<Integer> evenNumRdd = numJavaRDD.filter(
			new Function<Integer,Boolean>() {
				public static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Integer v1) throws Exception {
					return v1 % 2 == 0;
				} 
			
		});
		
		evenNumRdd.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
			
		});
		
		sc.close();
	}
	
	/*
	 * flatMap案例：将文本行拆分为多个单词
	 */
	private static void flatMap() {
		SparkConf conf = new SparkConf()
				.setAppName("flatMap")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<String> lineList = Arrays.asList("hello you","hello me","hello world");
		 
		//并行化集合，创建初始RDD
		JavaRDD<String> lines = sc.parallelize(lineList);
		
		//使用flatMap算子，将每行文本拆分成多个单词
		//flatMap算子，接收的参数是FlatMapFunction
		//需要自定义第二个泛型类型，call()返回的是iterator
		//flatMap接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回多个元素
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		
		words.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();
	}
	
	/*
	 * groupByKey案例：按照班级对成绩进行分组
	 */
	
	private static void groupByKey() {
		SparkConf conf = new SparkConf()
				.setAppName("groupByKey")
				.setMaster("local");
		
		//创建JavaSparkConf
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1",80),
				new Tuple2<String, Integer>("class2",75),
				new Tuple2<String, Integer>("class1",90),
				new Tuple2<String, Integer>("class2",65));
		 
		//并行化集合，创建初始RDD
		JavaPairRDD<String, Integer> scorePairRDD = sc.parallelizePairs(scores);
		
		//使用flatMap算子，将每行文本拆分成多个单词
		//flatMap算子，接收的参数是FlatMapFunction
		//需要自定义第二个泛型类型，call()返回的是iterator
		//flatMap接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回多个元素
		JavaPairRDD<String, Iterable<Integer>> groupScoreRdd = scorePairRDD.groupByKey();
		
		groupScoreRdd.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
			
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("class: " + t._1 );
				Iterator<Integer> iterator = t._2.iterator();
				while(iterator.hasNext()) {
					System.out.println(iterator.next());
				}
				System.out.println("=====================================");
			}
		});
		
		sc.close();
	}
	
	/*
	 * reduceByKey案例：计算每个班级的总分
	 */
	
	private static void reduceByKey() {
		SparkConf conf = new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		//创建JavaSparkConf
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合 
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1",80),
				new Tuple2<String, Integer>("class2",75),
				new Tuple2<String, Integer>("class1",90),
				new Tuple2<String, Integer>("class2",65));
		 
		//并行化集合，创建初始RDD
		JavaPairRDD<String, Integer> scorePairRDD = sc.parallelizePairs(scores);
		
		//使用reduceByKey算子
		//reduceByKey算子，接收的参数是Function2，有三个泛型参数
		//第一个和第二个泛型类型，代表了原始RDD中的元素value的类型
		//因此对每个key进行reduce，都会一次将第一个、第二个value传入，将值与第三个value传入
		//第三个泛型类型，代表了每次reduce操作的返回值的类型，默认也是与原始RDD的value类型相同
		JavaPairRDD<String, Integer> totalScores = scorePairRDD.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}
		);
		
		totalScores.foreach(
				new VoidFunction<Tuple2<String,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> t) throws Exception {	
						System.out.println(t._1 +": "+t._2);
					}
				});
		
		sc.close();
	}
	
	/*
	 * sortByKey案例：按照学生分数排序
	 */
	private static void sortByKey() {
		SparkConf conf = new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		//创建JavaSparkConf
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		List<Tuple2<Integer, String>> scoreList = Arrays.asList(
				new Tuple2<Integer, String>(100,"John"),
				new Tuple2<Integer, String>(70, "Jack"),
				new Tuple2<Integer, String>(85, "July"),
				new Tuple2<Integer, String>(50, "Tom"));
		
		//并行化创建RDD
		JavaPairRDD<Integer, String> scoresRdd = sc.parallelizePairs(scoreList);
		
		//对scoresRdd执行sortedByKey算子操作
		//默认是true即升序排序，false即为降序排序
		JavaPairRDD<Integer, String> sortedScoreRdd = scoresRdd.sortByKey(false);
		
		//打印结果
		sortedScoreRdd.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1 + ": " + t._2);
			}
		});
		
		//关闭SparkConf
		sc.close();
	}
	
	private static void joinAndCogroup() {
		SparkConf conf = new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		//创建JavaSparkConf
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer, String>(1,"Leo"),
				new Tuple2<Integer, String>(2, "Jack"),
				new Tuple2<Integer, String>(3, "Tom"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1,90),
				new Tuple2<Integer, Integer>(2, 80),
				new Tuple2<Integer, Integer>(3, 100),
				new Tuple2<Integer, Integer>(1,70),
				new Tuple2<Integer, Integer>(2, 50),
				new Tuple2<Integer, Integer>(3, 90));
		
		//并行化两个RDD
		JavaPairRDD<Integer, String> studentsJavaPairRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> scoreJavaPairRDD = sc.parallelizePairs(scoreList);
		
		//使用join算子关联两个RDD
		JavaPairRDD<Integer,Tuple2<String, Integer>> studentScores = studentsJavaPairRDD.join(scoreJavaPairRDD);
		
		//使用cogroup算子关联两个RDD
		//cogroup与join不同，相当于一个key join的所有value，都存入一个iterable中
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores2 = studentsJavaPairRDD.cogroup(scoreJavaPairRDD);

		
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> v) throws Exception {
				System.out.println("Student id: " + v._1);
				System.out.println("Student name: " + v._2._1);
				System.out.println("Student score: " + v._2._2);
				System.out.println("==========================");
			}
		});
		
		studentScores2.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t1) throws Exception {
				System.out.println("Student id: " + t1._1);
				System.out.println("Student name: " + t1._2._1);
				System.out.println("Student scores: " + t1._2._2);
				System.out.println("==========================");
			}
		});
		
		
		sc.close();
	}
	
	public static void main(String[] args) {
		//map();
		//filter();
		//flatMap();
		//groupByKey();
		//reduceByKey();
		//sortByKey();
		joinAndCogroup();
	}
}
