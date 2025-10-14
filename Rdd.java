Spark is a powerful big data processing framework test
Apache Spark provides high level APIs in Java Scala Python and R
It supports in-memory computation for faster processing test
The SparkContext is the entry point for using Spark test
RDD stands for Resilient Distributed Dataset test
Each RDD represents a collection of elements partitioned across cluster nodes
Transformations like map and filter are lazy operations test
Actions like count and collect trigger computation
Spark can run locally or on a cluster test
Java developers can use Spark's JavaRDD API for distributed data processing test// Simple Spark RDD Example (with print outputs)
// Reads files from C:\test, filters lines ending with "test",
// gets first word in uppercase, and saves output to C:\test-out-3

import org.apache.spark.api.java.*;
import org.apache.spark.*;
import java.util.List;

public class SimpleRDDExample {
    public static void main(String[] args) {

        // 1️⃣ Create Spark configuration and context
        SparkConf conf = new SparkConf()
                .setAppName("SimpleRDDExample")
                .setMaster("local[*]"); // run Spark locally
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2️⃣ Read all files from C:\test
        JavaRDD<String> lines = sc.textFile("C:\\test\\*");
        System.out.println("\n--- All lines from C:\\test ---");
        lines.collect().forEach(System.out::println);

        // 3️⃣ Keep only lines ending with the word "test"
        JavaRDD<String> filtered = lines.filter(line -> line.trim().endsWith("test"));
        System.out.println("\n--- Lines ending with 'test' ---");
        filtered.collect().forEach(System.out::println);

        // 4️⃣ Take only the first word from each line
        JavaRDD<String> firstWords = filtered.map(line -> line.split("\\s+")[0]);
        System.out.println("\n--- First words ---");
        firstWords.collect().forEach(System.out::println);

        // 5️⃣ Convert to UPPERCASE
        JavaRDD<String> upperWords = firstWords.map(String::toUpperCase);
        System.out.println("\n--- Uppercase words ---");
        upperWords.collect().forEach(System.out::println);

        // 6️⃣ Make sure there are only 2 output files
        JavaRDD<String> result = upperWords.coalesce(2);

        // 7️⃣ Save output to C:\test-out-3
        result.saveAsTextFile("C:\\test-out-3");

        // 8️⃣ Stop Spark
        sc.close();

        System.out.println("\n✅ Done! Check C:\\test-out-3 for output (2 files).");
    }
          }          
