package org.hello.bigdata.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class HelloSparkStreaming {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("spark://quickstart.cloudera:7077").setAppName("HelloSpark");
		JavaStreamingContext streamContext = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = streamContext.("localhost", 9999);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
		wordCounts.print();
		streamContext.start();
		streamContext.awaitTermination();
		
	}
}
