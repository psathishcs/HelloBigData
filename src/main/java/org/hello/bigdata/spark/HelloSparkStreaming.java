package org.hello.bigdata.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class HelloSparkStreaming {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("spark://quickstart.cloudera:7077").setAppName("HelloSpark");
		JavaStreamingContext streamContext = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = streamContext.socketTextStream("localhost", 9999);
		JavaDStream<String> words = lines.filter(
				new Function<String, Boolean>(){
					public Boolean call(String x) { return x.contains("Hello");}
				}	
		);
		words.print();
		streamContext.start();
		streamContext.awaitTermination();
		
	}
}
