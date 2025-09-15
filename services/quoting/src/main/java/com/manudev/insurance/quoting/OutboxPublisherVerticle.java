package com.manudev.insurance.quoting;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class OutboxPublisherVerticle extends AbstractVerticle {
    private final PgPool pool;
    private final Producer<String, String> producer;

    public OutboxPublisherVerticle(PgPool pool, String bootstrap) {
        this.pool = pool;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void start() {
        vertx.setPeriodic(500, t -> drainOutbox());
    }

    private void drainOutbox() {
        pool.getConnection(ar -> {
            if (ar.failed()) return;
            SqlConnection conn = ar.result();
            conn
                    .preparedQuery("SELECT id, payload FROM quoting.outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 50")
                    .execute(rows -> {
                        if (rows.failed()) { conn.close(); return; }
                        List<Row> batch = new ArrayList<>();
                        rows.result().forEach(batch::add);

                        if (batch.isEmpty()) { conn.close(); return; }

                        // publicar en Kafka y marcar publicados
                        for (Row r : batch) {
                            JsonObject payload = r.getJsonObject("payload");
                            String key = String.valueOf(payload.getJsonObject("data").getLong("quoteId"));
                            producer.send(new ProducerRecord<>("quote.created", key, payload.encode()));
                        }
                        producer.flush();

                        // marcar publicados
                        conn.preparedQuery(
                                        "UPDATE quoting.outbox_events SET published_at=NOW() WHERE id = ANY($1)")
                                .execute(Tuple.of(batch.stream().map(row -> row.getLong("id")).toArray(Long[]::new)),
                                        upd -> conn.close());
                    });
        });
    }

    @Override
    public void stop() {
        producer.flush();
        producer.close(Duration.ofSeconds(2));
    }
}
