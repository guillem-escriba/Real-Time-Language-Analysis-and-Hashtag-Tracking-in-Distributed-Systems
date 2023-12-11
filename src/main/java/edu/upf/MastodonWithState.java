package edu.upf;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.util.LanguageMapUtils;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MastodonWithState {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Real-time Mastodon With State");
        AppConfig appConfig = AppConfig.getConfig();
        int duration = 20;

        StreamingContext sc = new StreamingContext(conf, Durations.seconds(duration));
        JavaStreamingContext jsc = new JavaStreamingContext(sc);
        jsc.checkpoint("/tmp/checkpoint");

        JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

        // TODO IMPLEMENT ME
        String pathtomap = args[0];
        String language = args[1];


        int windowSize = 3; // Number of micro-batches of the window
        
        final JavaDStream<SimplifiedTweetWithHashtags> windowedStream = stream.window(Durations.seconds(windowSize*duration)); // Set window size 

        JavaRDD<String> inputRDD = jsc.sparkContext().textFile(pathtomap);
        JavaPairRDD<String, String> languageMapRDD = LanguageMapUtils.buildLanguageMap(inputRDD)
            .filter(splits -> splits._1().equals(language));

        // MICRO-BATCH
        JavaPairDStream<String, Tuple2<String, Integer>> tweetLanguageDStream = stream.mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), new Tuple2<>(tweet.getUserName(), 1)));
        JavaPairDStream<String, Tuple2<Tuple2<String,Integer>,String>> tweetLanguageWithNameDStream = tweetLanguageDStream
            .transformToPair(rdd -> rdd.join(languageMapRDD))
            .filter(tuple -> tuple._2()._1()._1() != null && !tuple._2()._1()._1().isEmpty());

        // Sum up the counts of tweets for each language and sort the output by the count in descending order
        JavaPairDStream<Integer, String> languageCountsDStream = tweetLanguageWithNameDStream
            .mapToPair(kv -> new Tuple2<>(kv._2()._1()._1(), kv._2()._1()._2()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(Tuple2::swap)
            .transformToPair(rdd -> rdd.sortByKey(false));


        // WINDOW
        
        JavaPairDStream<String, Tuple2<String, Integer>> tweetLanguageDStreamWindow = windowedStream.mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), new Tuple2<>(tweet.getUserName(), 1)));
        JavaPairDStream<String, Tuple2<Tuple2<String,Integer>,String>> tweetLanguageWithNameDStreamWindow = tweetLanguageDStreamWindow
            .transformToPair(rdd -> rdd.join(languageMapRDD))
            .filter(tuple -> tuple._2()._1()._1() != null && !tuple._2()._1()._1().isEmpty());
        
        // Sum up the counts of tweets for each language and sort the output by the count in descending order
        JavaPairDStream<Integer, String> languageCountsDStreamWindow = tweetLanguageWithNameDStreamWindow
            .mapToPair(kv -> new Tuple2<>(kv._2()._1()._1(), kv._2()._1()._2()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(Tuple2::swap)
            .transformToPair(rdd -> rdd.sortByKey(false));

        // Print the language counts to the console
        languageCountsDStream.print(20);
        languageCountsDStreamWindow.print(20); 

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }

}
