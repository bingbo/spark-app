package com.ibingbo.spark.app.sql;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by bing on 17/7/26.
 */
public class JavaSparkHiveExample {

    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("simple")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql("create table if not exists src (key int,value string) using hive");
        sparkSession.sql("load data local inpath 'src/main/resources/kv1.txt' into table src");
        sparkSession.sql("select * from src").show();
        sparkSession.sql("select count(*) from src").show();

        Dataset<Row> sqlDF = sparkSession.sql("select key,value from src where key<10 order by key");
        Dataset<String> stringDS =
                sqlDF.map((MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1)
                        , Encoders.STRING());
        stringDS.show();

        List<Record> records = new ArrayList<>();
        for (int key=1;key<100;key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = sparkSession.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");
        sparkSession.sql("select * from records r join src s on r.key = s.key").show();

        sparkSession.stop();
    }

}
