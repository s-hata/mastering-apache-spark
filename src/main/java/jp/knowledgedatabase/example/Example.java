package jp.knowledgedatabase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;

class Example {
  public static void main(String... args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile("src/main/resources/shakespeare.txt");
    JavaPairRDD<String, Integer> counts = file
        .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b);
    counts.foreach(p -> System.out.println(p));
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("/tmp/shakespeareWordCount");
  }
}
