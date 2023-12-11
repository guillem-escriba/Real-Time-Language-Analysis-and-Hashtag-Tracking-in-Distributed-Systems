package edu.upf;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.util.LanguageMapUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MastodonStateless {
    public static void main(String[] args) {
        String input = args[0];

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Stateless Exercise");
        AppConfig appConfig = AppConfig.getConfig();

        StreamingContext sc = new StreamingContext(conf, Durations.seconds(20));
        JavaStreamingContext jsc = new JavaStreamingContext(sc);
        jsc.checkpoint("/tmp/checkpoint");

        JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

        // TODO IMPLEMENT ME
        
        JavaRDD<String> inputRDD = jsc.sparkContext().textFile(input);
        JavaPairRDD<String, String> languageMapRDD = LanguageMapUtils.buildLanguageMap(inputRDD);
        //src/main/resources/map.tsv

        
        JavaPairDStream<String, Integer> tweetLanguageDStream = stream.mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), 1));

        JavaPairDStream<String, Tuple2<Integer, String>> tweetLanguageWithNameDStream = tweetLanguageDStream.transformToPair(rdd -> rdd.join(languageMapRDD));

        // Sum up the counts of tweets for each language and sort the output by the count in descending order
        JavaPairDStream<String, Integer> languageCountsDStream = tweetLanguageWithNameDStream
            .mapToPair(kv -> new Tuple2<>(kv._2()._2(), kv._2()._1()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(tuple -> tuple.swap())
            //.mapToPair(Tuple2::swap)
            .transformToPair(rdd -> rdd.sortByKey(false))
            //.mapToPair(Tuple2::swap)
            .mapToPair(tuple -> tuple.swap());

        // Print the language counts to the console
        languageCountsDStream.print();

        sc.start();
        sc.awaitTermination();
        
    }
}
