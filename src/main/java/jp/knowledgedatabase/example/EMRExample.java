package jp.knowledgedatabase;

import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class EMRExample {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    if (args.length < 2) { 
      System.err.println("Usage: JavaWordCount <inputFile> <outputFile>"); 
      System.exit(1);
    }

    SparkConf conf = new SparkConf().setAppName("Word Count");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> textFile = context.textFile(args[0]);
    JavaPairRDD<String, Integer> counts = textFile
      .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
      .mapToPair(s -> new Tuple2<>(s, 1))
      .reduceByKey((a, b) -> a + b);

    counts.saveAsTextFile(args[1]);
  } 
}
