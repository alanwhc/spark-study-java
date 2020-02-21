package com.spark.study.core;

/**
 * 并行化集合创建RDD
 * @author alanwang
 *
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import java.util.Arrays;
import java.util.List;

public class ParallelizeCollection {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("ParallelizeCollection")
				.setMaster("local");
		
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//通过并行化集合的方式创建RDD，那么就要调用SparkContext极其子类的parellelize()方法
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		//执行reduce算子操作
		//先进性1+2 = 3，在进行3 + 3 =6，以此类推
		int sum = numberRDD.reduce(new Function2<Integer,Integer,Integer>(){
			private static final long serialVersionUID = 1L;
			
			public Integer call(Integer num1, Integer num2) throws Exception{
				return num1 + num2;
			}
		});
		
		System.out.println(sum);
		
		//关闭SparkContext
		sc.close();
	}
}
