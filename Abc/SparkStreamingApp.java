package com.project;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class SparkStreamingApp {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // ✅ 1. Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("CustomerSpendingBehaviorAnalysis")
                .master("local[*]") // run locally
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ✅ 2. Read Stream from Kafka
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")  // change if your Kafka IP differs
                .option("subscribe", "transactions")                   // Kafka topic name
                .option("startingOffsets", "latest")
                .load();

        // ✅ 3. Define schema of transaction JSON
        StructType schema = new StructType()
                .add("transaction_id", DataTypes.StringType)
                .add("customer_id", DataTypes.StringType)
                .add("transaction_category", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("timestamp", DataTypes.TimestampType);

        // ✅ 4. Parse Kafka messages (value -> JSON)
        Dataset<Row> transactions = kafkaStream
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*");

        // ✅ 5. Compute aggregates (avg, total, frequency)
        Dataset<Row> aggDF = transactions
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                        window(col("timestamp"), "5 minutes"),
                        col("customer_id"),
                        col("transaction_category")
                )
                .agg(
                        avg("amount").alias("avg_spend"),
                        sum("amount").alias("total_spend"),
                        count("transaction_id").alias("frequency")
                );

        // ✅ 6. Rank Top 10 Spenders per Window
        WindowSpec windowSpec = Window.partitionBy("window").orderBy(col("total_spend").desc());
        Dataset<Row> topSpenders = aggDF
                .withColumn("rank", row_number().over(windowSpec))
                .filter(col("rank").leq(10))
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("customer_id"),
                        col("transaction_category"),
                        col("avg_spend"),
                        col("total_spend"),
                        col("frequency"),
                        col("rank")
                );

        // ✅ 7. MySQL connection properties
        String jdbcUrl = "jdbc:mysql://localhost:3306/customerdb";  // Replace with your DB name
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "YOUR_MYSQL_PASSWORD"); // <-- Replace this
        props.put("driver", "com.mysql.cj.jdbc.Driver");

        // ✅ 8. Write Stream output to MySQL (foreachBatch)
        StreamingQuery query = topSpenders.writeStream()
                .outputMode("update")   // or "complete"
                .trigger(Trigger.ProcessingTime("1 minute"))
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .mode(SaveMode.Append)
                            .jdbc(jdbcUrl, "customer_category_agg", props);
                })
                .start();

        // ✅ 9. Wait for termination
        try {
            query.awaitTermination();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
