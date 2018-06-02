package org.hello.bigdata.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function; 


public class HelloSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("").setAppName("HelloSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("README.md");
		JavaRDD<String> helloLines =  lines.filter(
			new Function<String, Boolean>(){
				public Boolean call(String x) { return x.contains("Hello");}
			}	
		);
		System.out.println(helloLines.count());
		for(String line :helloLines.take(10)) { System.out.println(line);}
	}
}
