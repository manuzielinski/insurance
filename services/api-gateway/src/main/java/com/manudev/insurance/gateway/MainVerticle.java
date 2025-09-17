package com.manudev.insurance.gateway;

import io.vertx.core.*;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.client.WebClient;

import java.util.UUID;

public class MainVerticle extends AbstractVerticle {
    private WebClient client;
    private String quotingHost;
    private int quotingPort;

    @Override
    public void start(Promise<Void> startPromise) {
        client = WebClient.create(vertx);
        quotingHost = System.getenv().getOrDefault("QUOTING_HOST", "localhost");
        quotingPort = Integer.parseInt(System.getenv().getOrDefault("QUOTING_PORT", "8081"));

        Router r = Router.router(vertx);
        r.route().handler(BodyHandler.create());

        // Health
        r.get("/health").handler(ctx -> ctx.json(new JsonObject().put("status", "UP")));

        // Middleware: traceId
        r.route().handler(ctx -> {
            String traceId = ctx.request().getHeader("X-Trace-Id");
            if (traceId == null || traceId.isBlank()) traceId = UUID.randomUUID().toString();
            ctx.put("traceId", traceId);
            ctx.response().putHeader("X-Trace-Id", traceId);
            ctx.next();
        });

        // Proxy: POST /quotes  ->  http://QUOTING/quotes
        r.post("/quotes").handler(ctx -> {
            String traceId = ctx.get("traceId");
            client.post(quotingPort, quotingHost, "/quotes")
                    .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader("X-Trace-Id", traceId)
                    .sendBuffer(ctx.body().buffer())
                    .onSuccess(res -> ctx.response().setStatusCode(res.statusCode()).end(res.bodyAsBuffer()))
                    .onFailure(err -> ctx.response().setStatusCode(502).end("quoting unavailable: " + err.getMessage()));
        });

        // Proxy: GET /quotes/:id  ->  http://QUOTING/quotes/:id
        r.get("/quotes/:id").handler(ctx -> {
            String traceId = ctx.get("traceId");
            client.get(quotingPort, quotingHost, "/quotes/" + ctx.pathParam("id"))
                    .putHeader("X-Trace-Id", traceId)
                    .send()
                    .onSuccess(res -> ctx.response().setStatusCode(res.statusCode()).end(res.bodyAsBuffer()))
                    .onFailure(err -> ctx.response().setStatusCode(502).end("quoting unavailable: " + err.getMessage()));
        });

        vertx.createHttpServer().requestHandler(r).listen(8080)
                .onSuccess(s -> startPromise.complete())
                .onFailure(startPromise::fail);
    }
}
