package com.manudev.insurance.quoting;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.pgclient.*;
import io.vertx.sqlclient.*;

import java.util.UUID;

public class MainVerticle extends AbstractVerticle {
    private PgPool pool;

    @Override
    public void start(Promise<Void> startPromise) {
        var connect = new PgConnectOptions()
                .setHost(System.getenv().getOrDefault("PGHOST","localhost"))
                .setPort(Integer.parseInt(System.getenv().getOrDefault("PGPORT","5432")))
                .setDatabase(System.getenv().getOrDefault("PGDATABASE","brokerdb"))
                .setUser(System.getenv().getOrDefault("PGUSER","postgres"))
                .setPassword(System.getenv().getOrDefault("PGPASSWORD","postgres"));
        pool = PgPool.pool(vertx, connect, new PoolOptions().setMaxSize(10));

        Router r = Router.router(vertx);
        r.route().handler(BodyHandler.create());

        r.post("/quotes").handler(ctx -> {
            JsonObject b = ctx.body().asJsonObject();
            String customerId = b.getString("customerId");
            String productCode = b.getString("productCode");
            Double premium = b.getDouble("premium");
            String traceId = b.getString("traceId", UUID.randomUUID().toString());

            if (customerId == null || productCode == null || premium == null) {
                ctx.response().setStatusCode(400).end("Campos obligatorios: customerId, productCode, premium");
                return;
            }

            vertx.executeBlocking(promise -> {
                        pool.withTransaction(tx ->
                                tx.preparedQuery(
                                                "INSERT INTO quoting.quotes(customer_id,product_code,premium,trace_id) VALUES($1,$2,$3,$4) RETURNING id")
                                        .execute(Tuple.of(customerId, productCode, premium, traceId))
                                        .compose(rows -> {
                                            long id = rows.iterator().next().getLong("id");
                                            JsonObject event = new JsonObject()
                                                    .put("eventId", UUID.randomUUID().toString())
                                                    .put("eventType", "quote.created")
                                                    .put("version", "1.0")
                                                    .put("traceId", traceId)
                                                    .put("data", new JsonObject()
                                                            .put("quoteId", id)
                                                            .put("customerId", customerId)
                                                            .put("productCode", productCode)
                                                            .put("premium", premium));
                                            return tx.preparedQuery(
                                                            "INSERT INTO quoting.outbox_events(event_id,event_type,payload) VALUES($1,$2,$3)")
                                                    .execute(Tuple.of(event.getString("eventId"), "quote.created", event));
                                        })
                        ).onComplete(promise);
                    }).onSuccess(v -> ctx.response().setStatusCode(201).end())
                    .onFailure(err -> ctx.response().setStatusCode(500).end(err.getMessage()));
        });

        r.get("/health").handler(ctx -> ctx.end("UP"));
        vertx.createHttpServer().requestHandler(r).listen(8081).onSuccess(s -> startPromise.complete())
                .onFailure(startPromise::fail);
    }
}
