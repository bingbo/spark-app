package com.ibingbo.spark.app.job;

import java.io.IOException;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Created by bing on 17/8/3.
 */
public class JavaSparkLauncherExample {
    public static void main(String[] args) throws Exception {
//        launcher_1();
        launcher_2();
    }

    public static void launcher_1() throws IOException {
        SparkAppHandle handle = new SparkLauncher()
                .setAppResource("/Users/zhangbingbing/Documents/github/spark-app/target/spark-app-1.0-SNAPSHOT.jar")
                .setMainClass("com.ibingbo.spark.app.App")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .startApplication();
        handle.addListener(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                System.out.println(handle.getAppId() + "----" + handle.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
                System.out.println(handle.getAppId() + "----" + handle.getState());
            }
        });
    }

    public static void launcher_2() throws Exception {
        Process spark = new SparkLauncher()
                .setAppResource("/Users/zhangbingbing/Documents/github/spark-app/target/spark-app-1.0-SNAPSHOT.jar")
                .setMainClass("com.ibingbo.spark.app.App")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .launch();
        spark.waitFor();
    }
}
