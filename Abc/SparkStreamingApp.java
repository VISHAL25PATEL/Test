package com.project;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;

public class SparkStreamingApp {
    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("CustomerSpendingAggregation")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "3")
                .getOrCreate();

        StructType schema = new StructType()
                .add("customer_id", DataTypes.StringType)
                .add("transaction_category", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "transactions")
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> parsed = raw.selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                .withColumn("ts", to_timestamp(col("timestamp").divide(1000)));

        Dataset<Row> agg = parsed
                .withWatermark("ts", "10 minutes")
                .groupBy(window(col("ts"), "1 hour"), col("customer_id"), col("transaction_category"))
                .agg(
                        sum("amount").alias("total_spend"),
                        avg("amount").alias("avg_spend"),
                        count(lit(1)).alias("frequency")
                );

        Dataset<Row> flattened = agg.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("customer_id"),
                col("transaction_category"),
                col("total_spend"),
                col("avg_spend"),
                col("frequency")
        );

        String jdbcUrl = "jdbc:mysql://localhost:3306/reports?useSSL=false&serverTimezone=UTC";
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "YOUR_MYSQL_PASSWORD");
        props.put("driver", "com.mysql.cj.jdbc.Driver");

        StreamingQuery query = flattened.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .mode(SaveMode.Append)
                            .jdbc(jdbcUrl, "customer_category_agg", props);

                    Dataset<Row> topSpenders = batchDF
                            .withColumn("rank", row_number().over(
                                    Window.partitionBy(col("window_start"), col("window_end"))
                                          .orderBy(col("total_spend").desc())))
                            .filter(col("rank").leq(10))
                            .select("window_start", "window_end", "customer_id", "total_spend", "rank");

                    topSpenders.write()
                            .mode(SaveMode.Append)
                            .jdbc(jdbcUrl, "top_spenders", props);
                })
                .option("checkpointLocation", "/tmp/spark-checkpoints/customer_spending")
                .start();

        query.awaitTermination();
    }
}
