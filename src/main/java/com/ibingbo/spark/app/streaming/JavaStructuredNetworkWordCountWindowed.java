package com.ibingbo.spark.app.streaming;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import scala.Tuple2;

/**
 * Created by bing on 17/7/26.
 */
public class JavaStructuredNetworkWordCountWindowed {
    public static void main(String[] args) throws StreamingQueryException {
        String host = "localhost";
        int port = 9999;
        int windowSize = 10;
        int slideSize = 5;

        String windowDuration = windowSize + " seconds";
        String slideDuration = slideSize + " seconds";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("simple")
                .master("local")
                .getOrCreate();

        Dataset<Row> lines = sparkSession
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .option("includeTimestamp", true)
                .load();

        Dataset<Row> words = lines.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");

        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("timestamp"), windowDuration, slideDuration), words.col("word")).count()
                .orderBy("window");

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
