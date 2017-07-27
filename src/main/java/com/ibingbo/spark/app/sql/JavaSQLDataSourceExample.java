package com.ibingbo.spark.app.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by bing on 17/7/26.
 */
public class JavaSQLDataSourceExample {

    private static final String DATA_PATH_PREFIX = "src/main/resources/";
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("simple")
                .master("local")
                .config("spark.some.config.option","some-value")
                .getOrCreate();

//        runParquetSchemaMergingExample(spark);
//        runBasicDataSourceExample(spark);
//        runBasicParquetExample(spark);
//        runJsonDatasetExample(spark);
        runJdbcDatasetExample(spark);
        spark.stop();
    }

    private static void runJdbcDatasetExample(SparkSession spark) {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "123456");
        Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:mysql://127.0.0.1:3306/test", "user", properties);
        jdbcDF.show();

        jdbcDF.write().option("createTableColumnTypes","comments varchar(100)").jdbc("jdbc:mysql://127.0.0.1:3306/test",
        "user",properties);

        jdbcDF.show();
    }

    private static void runJsonDatasetExample(SparkSession spark) {
        Dataset<Row> people = spark.read().json(DATA_PATH_PREFIX + "people.json");
        people.show();
        people.createOrReplaceTempView("people");

        Dataset<Row> namesDF = spark.sql("select name from people where age between 13 and 19");
        namesDF.show();

        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        anotherPeople.show();

    }
    private static void runBasicParquetExample(SparkSession spark) {
        Dataset<Row> peopleDF = spark.read().json(DATA_PATH_PREFIX + "people.json");
        peopleDF.show();
        peopleDF.write().parquet(DATA_PATH_PREFIX + "people.parquet");

        Dataset<Row> parquetFileDF = spark.read().parquet(DATA_PATH_PREFIX + "people.parquet");
        parquetFileDF.show();
        parquetFileDF.createOrReplaceTempView("parquetFIle");
        Dataset<Row> namesDF = spark.sql("select name from parquetFile where age between 13 and 19");
        Dataset<String> namesDS =
                namesDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
        namesDS.show();
    }
    private static void runBasicDataSourceExample(SparkSession spark) {
        Dataset<Row> usersDF = spark.read().load(DATA_PATH_PREFIX + "users.parquet");
        usersDF.show();
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

        Dataset<Row> peopleDF = spark.read().format("json").load(DATA_PATH_PREFIX+"people.json");
        peopleDF.show();
        peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

        Dataset<Row> sqlDF = spark.sql("select * from parquet.`src/main/resources/users.parquet`");
        sqlDF.show();

        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");

        usersDF.write().partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet");

        peopleDF.write().partitionBy("favorite_color").bucketBy(42, "name").saveAsTable
                ("people_partitioned_bucketed");

        spark.sql("drop table if exists people_bucketed");
        spark.sql("drop table if exists people_partitioned_bucketed");
    }

    private static void runParquetSchemaMergingExample(SparkSession spark) {
        List<Square> squares = new ArrayList<>();
        for (int value=1;value<=5;value++) {
            Square square = new Square();
            square.setValue(value);
            square.setValue(value * value);
            squares.add(square);
        }

        Dataset<Row> squareDF = spark.createDataFrame(squares, Square.class);
        squareDF.write().parquet(DATA_PATH_PREFIX + "data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value=6;value<=10;value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        Dataset<Row> cubeDF = spark.createDataFrame(cubes, Cube.class);
        cubeDF.write().parquet(DATA_PATH_PREFIX + "data/test_table/key=2");

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet(DATA_PATH_PREFIX + "data/test_table");
        mergedDF.printSchema();
        mergedDF.show();

    }


    public static class Square implements Serializable {
        private int value;
        private int square;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getSquare() {
            return square;
        }

        public void setSquare(int square) {
            this.square = square;
        }
    }

    public static class Cube implements Serializable {
        private int value;
        private int cube;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
    }

}
