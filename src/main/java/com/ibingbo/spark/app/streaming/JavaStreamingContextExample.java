package com.ibingbo.spark.app.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * Created by bing on 17/8/3.
 */
public class JavaStreamingContextExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("simple").setMaster("local");
        StreamingContext context = new StreamingContext(conf, Seconds.apply(5L));
        DStream<String> dStream = context.textFileStream("/Users/zhangbingbing/tmp/data");
        dStream.print();
        context.start();
        context.awaitTermination();
    }
}
