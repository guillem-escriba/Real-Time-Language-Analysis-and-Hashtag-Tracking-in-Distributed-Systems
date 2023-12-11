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

public class MastodonWindows {
    public static void main(String[] args) {
        String input = args[0];

        SparkConf conf = new SparkConf().setAppName("Real-time Mastodon Stateful with Windows Exercise");
        AppConfig appConfig = AppConfig.getConfig();

        int duration = 20; // Seconds of duration of the micro-batch. 

        StreamingContext sc = new StreamingContext(conf, Durations.seconds(duration));
        JavaStreamingContext jsc = new JavaStreamingContext(sc);
        jsc.checkpoint("/tmp/checkpoint");

        JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

        // TODO IMPLEMENT ME
        int windowSize = 3; // Number of micro-batches of the window
        
        final JavaDStream<SimplifiedTweetWithHashtags> windowedStream = stream.window(Durations.seconds(windowSize*duration)); // Set window size 

        JavaRDD<String> inputRDD = jsc.sparkContext().textFile(input);
        JavaPairRDD<String, String> languageMapRDD = LanguageMapUtils.buildLanguageMap(inputRDD);
        //src/main/resources/map.tsv

        // MICRO-BATCH
        JavaPairDStream<String, Integer> tweetLanguageDStream = stream.mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), 1));
        JavaPairDStream<String, Tuple2<Integer, String>> tweetLanguageWithNameDStream = tweetLanguageDStream.transformToPair(rdd -> rdd.join(languageMapRDD));

        // Sum up the counts of tweets for each language and sort the output by the count in descending order
        JavaPairDStream<String, Integer> languageCountsDStream = tweetLanguageWithNameDStream
            .mapToPair(kv -> new Tuple2<>(kv._2()._2(), kv._2()._1()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(Tuple2::swap)
            .transformToPair(rdd -> rdd.sortByKey(false))
            .mapToPair(Tuple2::swap);


        

        // WINDOW
        JavaPairDStream<String, Integer> tweetLanguageDStreamWindow = windowedStream.mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), 1));
        JavaPairDStream<String, Tuple2<Integer, String>> tweetLanguageWithNameDStreamWindow = tweetLanguageDStreamWindow.transformToPair(rdd -> rdd.join(languageMapRDD));
        
        // Sum up the counts of tweets for each language and sort the output by the count in descending order
        JavaPairDStream<String, Integer> languageCountsDStreamWindow = tweetLanguageWithNameDStreamWindow
            .mapToPair(kv -> new Tuple2<>(kv._2()._2(), kv._2()._1()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(Tuple2::swap)
            .transformToPair(rdd -> rdd.sortByKey(false))
            .mapToPair(Tuple2::swap);

        // Print the language counts to the console
        languageCountsDStream.print(15);
        languageCountsDStreamWindow.print(15); 

        // Start the application and wait for termination signal
        sc.start();
        sc.awaitTermination();
    }

}
