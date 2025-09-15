package com.manudev.insurance.notifications;

import io.vertx.core.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class NotificationsVerticle extends AbstractVerticle {
    private Consumer<String, String> consumer;

    @Override
    public void start() {
        Properties p = new Properties();
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_LOCAL", "localhost:9092");
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "notifications");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(p);
        consumer.subscribe(List.of("quote.created"));

        vertx.executeBlocking(promise -> {
            while (!Thread.currentThread().isInterrupted()) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(rec -> {
                    System.out.println("[NOTIFY] " + rec.value());
                    // aquí enviarías email/SMS; por ahora, log
                });
                consumer.commitAsync();
            }
        });
    }

    @Override
    public void stop() {
        try { consumer.wakeup(); } catch (Exception ignored) {}
        consumer.close();
    }
}
