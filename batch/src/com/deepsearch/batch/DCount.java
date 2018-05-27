package com.deepsearch.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.deepsearch.common.HBaseUtils;

import scala.Tuple2;

/**
 * 统计文档总数
 * */
public class DCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length < 1) {
			System.err.println("Usage: documents <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("DCount")
				.getOrCreate();
				
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		System.out.println("doucuments count : " + lines .count());
//		lines.foreach(s -> System.out.println(s));
		List<Tuple2<String, Integer>> output = new ArrayList<Tuple2<String, Integer>>();
		output.addAll(Arrays.asList(new Tuple2<>("article",Integer.parseInt(lines.count()+""))));
		HBaseUtils.pubBatch("d_batch", output, "cf", "count");
		spark.stop();
		
	}

}
