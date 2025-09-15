package com.manudev.insurance.notifications;

import io.vertx.core.Vertx;

public class Launcher {
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new NotificationsVerticle());
    }
}
