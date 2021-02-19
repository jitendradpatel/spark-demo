package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBCTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JDBC App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> jdbcDF = spark.read().format("jdbc")
                .option("url", "jdbc:oracle:thin:<USERNAME>/<PASSWORD>@//localhost:1521/<SID>")
                .option("dbtable", "<SCHEMANAME>.emp")
                .option("user", "<USERNAME>")
                .option("password", "<PASSWORD>")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("numPartitions", 5)
                .option("partitionColumn", "empno")
                .option("lowerBound", 1)
                .option("upperBound", 10)
                .load();
        jdbcDF.show();
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:<DBNAME>")
                .option("dbtable", "<SCHEMANAME>.emp")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("numPartitions", 5)
                .option("partitionColumn", "empno")
                .option("lowerBound", 1)
                .option("upperBound", 10)
                .save();
    }

    /* Column mapping example
    jdbcDF.write()
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
     */
}
