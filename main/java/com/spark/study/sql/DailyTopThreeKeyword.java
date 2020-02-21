package com.spark.study.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;


/**
 * 每日top3热点搜索词统计
 * @author alanwang
 *
 */

public class DailyTopThreeKeyword {
	public static void main(String[] args) throws AnalysisException {
		SparkConf conf = new SparkConf()
				.setAppName("DailyTopThreeKeyword");
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("DailyTopThreeKeyword")
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
		
		//1.从HDFS中获取数据，并创建RDD
		JavaRDD<String> rawRdd = sc.textFile("hdfs://master:9000/spark-study/keyword.txt");
		//JavaRDD<String> rawRdd = sc.textFile("/Users/alanwang/Documents/Work/textFile/keyword.txt");
		
		//2.在实际企业项目开发中，这个查询条件，是通过J2EE平台插入到某个MySQL表中的
		//通常会用Spring框架和ORM框架（MyBatis）去提取MySQL表中的查询条件
		Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
		queryParamMap.put("city", Arrays.asList("beijing"));
		queryParamMap.put("platform", Arrays.asList("android"));
		queryParamMap.put("version", Arrays.asList("1.0","1.2","1.5","2.0"));
		
		//将查询参数Map封装为一个广播边练，可以优化Worker节点，仅拷贝一份数据即可
		final Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sc.broadcast(queryParamMap);
		
		//使用查询参数Map构造的广播变量，进行筛选
		JavaRDD<String> filteredRdd = rawRdd.filter(
			new Function<String, Boolean>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String log) throws Exception {
					//获取城市、平台、版本的数据
					String[] logSplited = log.split(",");
					//System.out.println(logSplited[0]+","+logSplited[1]+","+logSplited[2]+","+logSplited[3]+","+logSplited[4]+","+logSplited[5]);
					String city= logSplited[3];
					String platform = logSplited[4];
					String version = logSplited[5];
					
					//与查询条件比较，如果city、platform、version三个条件中任何一个在List中存在
					//并且不满足queryParamMap中的筛选条件，则返回false，即被过滤掉
					//否则，返回true
					Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();
					List<String> cities = queryParamMap.get("city");
					if(cities.size() > 0 && !cities.contains(city)) 
						return false;
					List<String> platforms = queryParamMap.get("platform");
					if(platforms.size() > 0 && !platforms.contains(platform))
						return false;
					List<String> versions = queryParamMap.get("version");
					if(versions.size() > 0 && !versions.contains(version))
						return false;
					return true;
				}
			});
		
		//3.将过滤后的RDD映射为(日期_搜索词，用户）的格式，并进行分组
		JavaPairRDD<String, String> dateKeywordUserRdd = filteredRdd.mapToPair(
			new PairFunction<String, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(String log) throws Exception {
					String[] logSplited = log.split(",");
					String date = logSplited[0];
					String user = logSplited[1];
					String keyword = logSplited[2];
					return new Tuple2<String, String>(date+"_"+keyword, user);
				}
			});
		
		//分组，获取每天每个搜索词，有哪些用户搜索
		JavaPairRDD<String, Iterable<String>> dateKeywordUserGroupedRdd = dateKeywordUserRdd.groupByKey();
		
		//执行去重操作，获得uv
		JavaPairRDD<String, Long> dateKeywordUvRdd =  dateKeywordUserGroupedRdd.mapToPair(
			new PairFunction<Tuple2<String,Iterable<String>>, String, Long>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
					String dateKeyword = dateKeywordUsers._1;
					Iterator<String> users = dateKeywordUsers._2.iterator();
					
					//对用户去重，并统计去重后的数量
					List<String> distinctUsers = new ArrayList<String>();
					while(users.hasNext()) {
						String user = users.next();
						if(!distinctUsers.contains(user)) {
							distinctUsers.add(user);
						}
					}
					
					//获取uv
					long uv = distinctUsers.size();
					return new Tuple2<String, Long>(dateKeyword, uv);
				}
			});
		
		//4.将3中数据转换为类型为Row的RDD，并转换为DataFrame
		JavaRDD<Row> dateKeywordUvRowRdd = dateKeywordUvRdd.map(
			new Function<Tuple2<String,Long>, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
					return RowFactory.create(dateKeywordUv._1.split("_")[0],
									dateKeywordUv._1.split("_")[1],
									dateKeywordUv._2);
				}
			});
		List<StructField> structFields = Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("keyword", DataTypes.StringType, true),
				DataTypes.createStructField("uv", DataTypes.LongType, true));
		
		Dataset<Row> dateKeywordUvDs = sparkSession.createDataFrame(dateKeywordUvRowRdd, DataTypes.createStructType(structFields));
		
		//5.使用Spark SQL的开窗函数，统计每天搜索uv排名前三的热点搜索词
		dateKeywordUvDs.createTempView("daily_keyword_uv");
		Dataset<Row> dailyTopThreeKeywordDs = sparkSession.sql(""
				+ "SELECT date,keyword,uv "
				+ "FROM ("
					+ "SELECT "
						+ "date,"
						+ "keyword,"
						+ "uv,"
						+ "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
					+ "FROM daily_keyword_uv"  
				+ ") tmp "
				+ "WHERE rank<=3");
		
		//6.将DataFrame转换为RDD，然后映射计算出每天top3搜索词的uv总数
		JavaRDD<Row> dailyTopThreeKeywordRdd = dailyTopThreeKeywordDs.javaRDD();
		
		JavaPairRDD<String, String> topThreeDateKeywordUvRdd = dailyTopThreeKeywordRdd.mapToPair(
			new PairFunction<Row, String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(Row row) throws Exception {
					return new Tuple2<String, String>(String.valueOf(row.get(0)),
							String.valueOf(row.get(1) + "_" + String.valueOf(row.get(2))));
				}
			});
		
		JavaPairRDD<String, Iterable<String>> topThreeKeywordRdd = topThreeDateKeywordUvRdd.groupByKey();

		JavaPairRDD<Long, String> uvDateKeywordsRdd = topThreeKeywordRdd.mapToPair(
			new PairFunction<Tuple2<String,Iterable<String>>, Long, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> uvDateKeyword) throws Exception {
					String date = uvDateKeyword._1;
					Long totalUv = 0L;
					String dateKeyword = date;
					Iterator<String> keywordUvIterator = uvDateKeyword._2.iterator();
					while (keywordUvIterator.hasNext()) {
						String keywordUv = keywordUvIterator.next();
						totalUv += Long.valueOf(keywordUv.split("_")[1]);
						dateKeyword += "," + keywordUv;
					}
					return new Tuple2<Long, String>(totalUv, dateKeyword);
				}
			});
		
		
		//7.按照每天总uv进行倒序排序
		JavaPairRDD<Long, String> sortedUvDateKeywordRdd = uvDateKeywordsRdd.sortByKey(false);
		
		//8.映射为(日期，搜索词，uv)的格式
		JavaRDD<Row> sortedRowRdd = sortedUvDateKeywordRdd.flatMap(
			new FlatMapFunction<Tuple2<Long,String>, Row>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Row> call(Tuple2<Long, String> tuple) throws Exception {
					String[] dateKeywordSplited = tuple._2.split(",");
					List<Row> rows = new ArrayList<Row>();
					rows.add(RowFactory.create(dateKeywordSplited[0], //日期
							dateKeywordSplited[1].split("_")[0],//关键词
							Long.valueOf(dateKeywordSplited[1].split("_")[1]))); //uv
					rows.add(RowFactory.create(dateKeywordSplited[0], 
							dateKeywordSplited[2].split("_")[0],
							Long.valueOf(dateKeywordSplited[2].split("_")[1])));
					rows.add(RowFactory.create(dateKeywordSplited[0], 
							dateKeywordSplited[3].split("_")[0],
							Long.valueOf(dateKeywordSplited[3].split("_")[1])));
					return rows.iterator();
				}
			});
		
		//9.将rdd转换为dataframe，并存至hive表中
		Dataset<Row> resultDs = sparkSession.createDataFrame(sortedRowRdd, DataTypes.createStructType(structFields));
		//resultDs.show();
		
		//将结果存至hive表
		sparkSession.sql("DROP TABLE IF EXISTS daily_topthree_keyword_uv");
		resultDs.write().saveAsTable("daily_topthree_keyword_uv");
		
		sparkSession.catalog().dropTempView("daily_keyword_uv");
		sc.close();
	}
}
