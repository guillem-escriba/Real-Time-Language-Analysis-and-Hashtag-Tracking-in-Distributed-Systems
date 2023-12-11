package edu.upf;


import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.storage.DynamoHashTagRepository;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MastodonHashtags {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Real-time Mastodon Hashtags");
        AppConfig appConfig = AppConfig.getConfig();
        StreamingContext sc = new StreamingContext(conf, Durations.seconds(10));
        JavaStreamingContext jsc = new JavaStreamingContext(sc);
        jsc.checkpoint("/tmp/checkpoint");

        JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();
        
        // TODO IMPLEMENT ME
        DynamoHashTagRepository dynamoDB = new DynamoHashTagRepository(); // new dynamo instance
        stream.foreachRDD(rdd -> rdd.foreachPartition(partition -> partition.forEachRemaining(tweet -> dynamoDB.write(tweet)))); // writes each tweet to the dynamo, while the map method need to modify the rdd, the foreach method does not need to.
        
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}