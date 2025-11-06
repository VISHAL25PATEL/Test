package com.project;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        String topic = "transactions";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random rand = new Random();

        String[] customers = {"cust_001", "cust_002", "cust_003", "cust_004", "cust_005"};
        String[] categories = {"electronics", "groceries", "fashion", "travel", "utilities"};

        for (int i = 0; i < 500; i++) {
            Map<String, Object> trx = new HashMap<>();
            trx.put("customer_id", customers[rand.nextInt(customers.length)]);
            trx.put("transaction_category", categories[rand.nextInt(categories.length)]);
            trx.put("amount", Math.round((10 + rand.nextDouble() * 500) * 100.0) / 100.0);
            trx.put("timestamp", System.currentTimeMillis());

            String json = mapper.writeValueAsString(trx);
            producer.send(new ProducerRecord<>(topic, json));
            Thread.sleep(20);
        }

        producer.flush();
        producer.close();
        System.out.println("✅ Produced 500 sample transactions to Kafka topic 'transactions'.");
    }
}
