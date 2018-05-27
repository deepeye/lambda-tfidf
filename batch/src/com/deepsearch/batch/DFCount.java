package com.deepsearch.batch;

import java.util.List;
import java.util.regex.Pattern;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.deepsearch.common.HBaseUtils;

import scala.Tuple2;

//计算语料库词频数
public class DFCount {

	private static final Pattern DELIMIT = Pattern.compile("\t");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length < 1) {
			System.err.println("Usage: documents <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("DFCount")
				.getOrCreate();
		
		//文档id \t 文档内容 \t 文档来源
		//document \t documentId \t source
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		//参数是函数，函数应用于RDD每一个元素，将元素数据进行拆分，变成迭代器，返回值是新的RDD
		JavaRDD<Term> terms = lines.flatMap(s -> ToAnalysis.parse(DELIMIT.split(s)[1]).getTerms().iterator());
		//参数是函数，函数应用于RDD每一个元素，返回值是新的RDD
		JavaRDD<String> words = terms.map(s -> s.getRealName());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1)); 
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + ": " + tuple._2());
	    }
		HBaseUtils.pubBatch("df_batch", output, "cf", "count");
		
		spark.stop();
	}

}
