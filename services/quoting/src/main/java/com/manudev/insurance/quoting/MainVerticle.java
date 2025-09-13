package com.manudev.insurance.quoting;

import io.vertx.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
    @Override
    public void start() {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end("Quoting Service OK"))
                .listen(8081);
    }
}