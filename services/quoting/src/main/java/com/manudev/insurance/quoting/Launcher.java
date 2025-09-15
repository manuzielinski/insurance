package com.manudev.insurance.quoting;

import io.vertx.core.Vertx;
import io.vertx.pgclient.*;
import io.vertx.sqlclient.*;

public class Launcher {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        var connect = new PgConnectOptions()
                .setHost(System.getenv().getOrDefault("PGHOST","localhost"))
                .setPort(Integer.parseInt(System.getenv().getOrDefault("PGPORT","5432")))
                .setDatabase(System.getenv().getOrDefault("PGDATABASE","brokerdb"))
                .setUser(System.getenv().getOrDefault("PGUSER","postgres"))
                .setPassword(System.getenv().getOrDefault("PGPASSWORD","postgres"));
        PgPool pool = PgPool.pool(vertx, connect, new PoolOptions().setMaxSize(10));

        vertx.deployVerticle(new MainVerticle());
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_LOCAL", "localhost:9092");
        vertx.deployVerticle(new OutboxPublisherVerticle(pool, bootstrap));
    }
}
