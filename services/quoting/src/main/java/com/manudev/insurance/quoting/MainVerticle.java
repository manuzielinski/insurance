package com.manudev.insurance.quoting;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.redis.client.Command;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

import java.util.UUID;

public class MainVerticle extends AbstractVerticle {
    private PgPool pool;

    @Override
    public void start(Promise<Void> startPromise) {
        var connect = new PgConnectOptions()
                .setHost(System.getenv().getOrDefault("PGHOST", "localhost"))
                .setPort(Integer.parseInt(System.getenv().getOrDefault("PGPORT", "5432")))
                .setDatabase(System.getenv().getOrDefault("PGDATABASE", "brokerdb"))
                .setUser(System.getenv().getOrDefault("PGUSER", "postgres"))
                .setPassword(System.getenv().getOrDefault("PGPASSWORD", "postgres"));

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

            // Transacción no-bloqueante
            pool.withTransaction(tx ->
                            tx.preparedQuery(
                                            "INSERT INTO quoting.quotes (customer_id, product_code, premium, trace_id) " +
                                                    "VALUES ($1, $2, $3, $4) RETURNING id")
                                    .execute(Tuple.of(customerId, productCode, premium, traceId))
                                    .compose((RowSet<Row> rows) -> {
                                        Row row = rows.iterator().next();
                                        long id = row.getLong("id");

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

                                        // Guardamos en el outbox; si tu columna es JSONB, castea a ::jsonb
                                        return tx.preparedQuery(
                                                        "INSERT INTO quoting.outbox_events (event_id, event_type, payload) " +
                                                                "VALUES ($1, $2, $3::jsonb)")
                                                .execute(Tuple.of(event.getString("eventId"), "quote.created", event.encode()));
                                    })
                                    .mapEmpty() // Future<Void>
                    ).onSuccess(v -> ctx.response().setStatusCode(201).end())
                    .onFailure(err -> ctx.response().setStatusCode(500).end(err.getMessage()));
        });

        // ... dentro de start():
        r.get("/quotes/:id").handler(ctx -> {
            String id = ctx.pathParam("id");
            var redis = io.vertx.redis.client.Redis.createClient(vertx, System.getenv().getOrDefault("REDIS_URL","redis://localhost:6379"));
            String cacheKey = "quote:"+id;

            redis.connect()
                    .compose(conn -> conn.send(io.vertx.redis.client.Request.cmd(Command.create("GET")).arg(cacheKey))
                            .compose(resp -> {
                                if (resp != null && !resp.toString().equals("null")) {
                                    ctx.response().putHeader("Content-Type","application/json").end(resp.toString());
                                    return Future.succeededFuture();
                                }
                                // si no está en cache -> DB
                                return pool.preparedQuery("SELECT id, customer_id, product_code, premium, status, trace_id FROM quoting.quotes WHERE id=$1")
                                        .execute(Tuple.of(Long.valueOf(id)))
                                        .compose(rows -> {
                                            if (rows.size()==0) { ctx.response().setStatusCode(404).end(); return Future.succeededFuture(); }
                                            var r0 = rows.iterator().next();
                                            var json = new JsonObject()
                                                    .put("id", r0.getLong("id"))
                                                    .put("customerId", r0.getString("customer_id"))
                                                    .put("productCode", r0.getString("product_code"))
                                                    .put("premium", r0.getNumeric("premium"))
                                                    .put("status", r0.getString("status"))
                                                    .put("traceId", r0.getString("trace_id"));
                                            // guardar en cache con TTL 300
                                            return conn.send(io.vertx.redis.client.Request.cmd(Command.create("SETEX")).arg(cacheKey).arg(300).arg(json.encode()))
                                                    .onComplete(v -> {
                                                        ctx.response().putHeader("Content-Type","application/json").end(json.encode());
                                                    });
                                        });
                            })
                    ).onFailure(err -> ctx.response().setStatusCode(500).end(err.getMessage()));
        });


        r.get("/health").handler(ctx -> ctx.end("UP"));

        vertx.createHttpServer()
                .requestHandler(r)
                .listen(8081)
                .onSuccess(s -> startPromise.complete())
                .onFailure(startPromise::fail);
    }
}
