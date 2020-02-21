package com.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 分组取Top3
 * @author alanwang
 *
 */

public class GroupTopThree {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("GroupTopThree")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("/Users/alanwang/Documents/Work/textFile/score.txt");
		
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(
			new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					String[] lineSplited = line.split(" ");
 					return new Tuple2<String, Integer>(lineSplited[0],Integer.valueOf(lineSplited[1]));
				}
				
			});
		
		JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();
		
		JavaPairRDD<String, Iterable<Integer>> topThreeScores = groupedPairs.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classScores) throws Exception {
						Integer[] topThree = new Integer[3];
						String className =  classScores._1;
						
						Iterator<Integer> scores = classScores._2.iterator();
						while(scores.hasNext()) {
							Integer score = scores.next();
							
							for(int i = 0; i < 3; i++) {
								if(topThree[i] == null) {
									topThree[i] = score;
									break;
								} else if(score > topThree[i]) {
									for(int j = 2; j > i; j--) {
										topThree[j] = topThree[j - 1];  
									}
									
									topThree[i] = score;
									break;
								} 
							}
						}
						return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(topThree));
					}
				});
		
		topThreeScores.foreach(
			new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<String, Iterable<Integer>> classScore) throws Exception {
					System.out.println("class: "+ classScore._1);
					Iterator<Integer> scoreIterator = classScore._2.iterator();
					while(scoreIterator.hasNext()) {
						Integer score = scoreIterator.next();
						System.out.println(score);
					}
					System.out.println("=========================================");
				}
			});
		
		sc.close();
	}
}
