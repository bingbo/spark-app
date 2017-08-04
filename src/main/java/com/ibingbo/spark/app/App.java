package com.ibingbo.spark.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * Created by bing on 17/7/24.
 */
public class App {
    public static void main(String[] args) throws Exception{
//        runBasicExample();
        getTextFromHDFS();
    }

    private static void getTextFromHDFS() {
        SparkConf conf = new SparkConf().setAppName("simple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile("hdfs://szwg-ecomon-hdfs.dmop.baidu"
                + ".com:54310/app/dt/bigdata/dba/?config=dba-meta");
        System.out.println(data.first());
        data.collect().forEach(x -> System.out.println(x));
    }

    private static void runBasicExample() {
        SparkConf conf = new SparkConf().setAppName("simple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        distData.collect().forEach(x -> System.out.println(x));
        Integer result = distData.reduce((a, b) -> a + b);
        System.out.println("--------------" + result + "--------------");

        JavaRDD<String> distFile = sc.textFile("/Users/zhangbingbing/data.txt");
        distFile.persist(StorageLevel.MEMORY_ONLY());
        distFile.take(10).forEach(x -> System.out.println(x));
        result = distFile.map(new GetLength()).reduce(new Sum());
        System.out.println("--------------" + result + "--------------");

        JavaPairRDD<String, Integer> pairs = distFile.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        System.out.println(counts.sortByKey().count());

        Broadcast<int[]> broadcast = sc.broadcast(new int[] {1, 2, 3});
        System.out.println(broadcast.value().toString());

        LongAccumulator accumulator = sc.sc().longAccumulator();
        sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accumulator.add(x));
        System.out.println(accumulator.value());

        sc.stop();
    }

    static class GetLength implements Function<String, Integer> {
        @Override
        public Integer call(String s) throws Exception {
            return s.length();
        }
    }

    static class Sum implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2;
        }
    }

}
