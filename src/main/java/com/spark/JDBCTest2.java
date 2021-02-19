package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBCTest2 {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setMaster("spark://<HOSTNAME>:7077").setAppName("JDBC App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("/<FOLDERNAME>/postgresql-42.2.5.jar");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> accounts = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:example")
                .option("dbtable", "pgbench_accounts")
                .option("user", "postgres")
                .option("password", "postgres")
//                .option("numPartitions", 5)
//                .option("partitionColumn", "empno")
//                .option("lowerBound", 1)
//                .option("upperBound", 10)
                .load();
        System.out.println("Count: " + accounts.count());
        accounts.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:example")
                .option("dbtable", "pgbench_accounts2")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("numPartitions", 8)
                .option("partitionColumn", "empno")
                .option("lowerBound", 1)
                .option("upperBound", 1000000)
                .save();
        long endTime = System.currentTimeMillis();
        System.out.println("Total Time Taken: " + (endTime - startTime));
    }
}
