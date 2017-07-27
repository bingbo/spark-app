package com.ibingbo.spark.app.sql;

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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * Created by bing on 17/7/24.
 */
public class JavaSparkSQLExample {

    private static final SparkSession sparkSession = SparkSession
            .builder()
            .appName("Java Spark SQL basic example")
            .master("local")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();

    public static void main(String[] args) throws Exception{
        runBasicDataFrameExample();
        runDatasetCreationExample();
        runProgrammaticSchemaExample();
        runJdbcDatasetExample();

        sparkSession.stop();
    }

    private static void runProgrammaticSchemaExample() {
        JavaRDD<String> peopleRDD = sparkSession.sparkContext()
                .textFile("src/main/resources/people.txt", 1)
                .toJavaRDD();

        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = peopleRDD.map((Function<String,Row>)record->{
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);
        peopleDataFrame.createOrReplaceTempView("people");
        Dataset<Row> results = sparkSession.sql("select * from people");
        results.show();
        Dataset<String> namesDS =
                results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
        namesDS.show();
    }
    /**
     * 数据查询框架
     * @throws Exception
     */
    private static void runBasicDataFrameExample() throws Exception{
        Dataset<Row> df = sparkSession.read().json("src/main/resources/people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(df.col("name"),df.col("age").plus(1)).show();
        df.filter(df.col("age").gt(21)).show();
        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");

        System.out.println("---------------sql-------------------");
        Dataset<Row> sqlDF = sparkSession.sql("select * from people");
        sqlDF.show();

        System.out.println("---------------global_temp.people-------------------");
        df.createOrReplaceGlobalTempView("people");
        sparkSession.sql("select * from global_temp.people").show();

        sparkSession.newSession().sql("select * from global_temp.people").show();
    }

    /**
     * 数据写框架
     */
    private static void runDatasetCreationExample() {
        JavaRDD<Person> personJavaRDD = sparkSession.read()
                .textFile("src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        Dataset<Row> peopleDF = sparkSession.createDataFrame(personJavaRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = sparkSession.sql("select name from people where age between 13 and 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF =
                teenagersDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString
                        (0), stringEncoder);
        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }


    private static void runJdbcDatasetExample() throws Exception{
        String url = "jdbc:mysql://127.0.0.1:3306/test";
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url",url)
                .option("dbtable","user")
                .option("user","root")
                .option("password","123456")
                .load();
        jdbcDF.show();
        jdbcDF.printSchema();
    }




    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }


}
