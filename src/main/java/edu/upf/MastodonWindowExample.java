package edu.upf;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.util.LanguageMapUtils;
import scala.Tuple2;

import java.io.IOException;
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
import org.apache.spark.streaming.dstream.DStream;

public class MastodonWindowExample {

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        AppConfig appConfig = AppConfig.getConfig();
        StreamingContext sc = new StreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        sc.checkpoint("/tmp/checkpoint");

        final MastodonDStream stream = new MastodonDStream(sc, appConfig);

        final DStream<SimplifiedTweetWithHashtags> windowedStream = stream.window(Durations.seconds(15));
    
        
        stream.count().print();
        windowedStream.count().print();

        // Start the application and wait for termination signal
        sc.start();
        sc.awaitTermination();
    }
}
