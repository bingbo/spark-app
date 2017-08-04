# spark-app
spark-app

## spark安装

> 到http://spark.apache.org/downloads.html下载相应的版本，解压

```bash
tar xzvf spark-2.2.0-bin-hadoop2.7.tgz
```

## 启动shell交互

```bash
./bin/spark-shell

##基于scala语言
scala> val textFile = sc.textFile("README.md")
textFile: spark.RDD[String] = spark.MappedRDD@2ee9b6e3

##计算
scala> textFile.count() // Number of items in this RDD
res0: Long = 126

scala> textFile.first() // First item in this RDD
res1: String = # Apache Spark
```

## Java API使用

### 添加依赖


```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.0</version>
</dependency>

<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.11</artifactId>
    <version>2.2.0</version>
</dependency>

<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.2.0</version>
</dependency>

<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>2.2.0</version>
</dependency>

<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.11</artifactId>
    <version>2.2.0</version>
</dependency>
```

### 引用API编程

```java
public class App {
    public static void main(String[] args) throws Exception{
        runBasicExample();
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
```
## spark提交job

```bash
spark-submit --class com.ibingbo.spark.app.job.JavaSparkLauncherExample --master local target/spark-app-1.0-SNAPSHOT.jar  100
```
