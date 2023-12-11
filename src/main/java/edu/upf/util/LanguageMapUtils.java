package edu.upf.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
//        return null;// IMPLEMENT ME
        return lines
                .filter(line -> !line.startsWith("ISO")) // discard the header
                .map(l -> l.split("\t"))
                .filter(splits -> splits.length >= 3 && !splits[1].isEmpty()) // filter out lines with less than 3 columns (first one is always present and we need 2nd and 3rd), and that wanted code column is not empty)
                .mapToPair(splits -> Tuple2.apply(splits[1], splits[2])); //avoid having repeated languages
    }
}
