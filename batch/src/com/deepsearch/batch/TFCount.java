package com.deepsearch.batch;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.deepsearch.common.HBaseUtils;

import scala.Tuple2;

//计算语料库每篇文档词频
public class TFCount {

	private static final Pattern DELIMIT = Pattern.compile("\t");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length < 1) {
			System.err.println("Usage: documents <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("TFCount")
				.getOrCreate();
		
		//文档id \t 文档内容 \t 文档来源
		//documentId \t document \t source
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		JavaPairRDD<String, Integer> ones 
			= lines.flatMapToPair(s -> {
				List<Tuple2<String,Integer>> pairs = new LinkedList<>();
				String[] arr = DELIMIT.split(s);
				List<Term> terms = ToAnalysis.parse(arr[1]).getTerms();
				for(Term term : terms){
					String key = String.join("", arr[0], term.getRealName());
					pairs.add(new Tuple2<>(key, 1));
				}
				return pairs.iterator();
			});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<String, Integer>> output = counts.collect();
		for(Tuple2<?,?> tuple : output){
			System.out.println(tuple._1()+":"+tuple._2());
		}
		HBaseUtils.pubBatch("tf_batch", output, "cf", "count");
		spark.stop();
		
	}

}
