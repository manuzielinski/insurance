package com.manudev.insurance.notifications;

import io.vertx.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
    @Override
    public void start() {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end("Notifications Service OK"))
                .listen(8082);
    }
}
