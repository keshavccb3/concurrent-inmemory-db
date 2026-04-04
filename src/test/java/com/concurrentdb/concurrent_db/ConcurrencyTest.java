package com.concurrentdb.concurrent_db;


import com.concurrentdb.concurrent_db.Service.DatabaseService;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrencyTest {

    public static void main(String[] args) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 20; i++) {
            int id = i;

            executor.submit(() -> {
                try {
                    String url = "http://localhost:8080/db/users/1";

                    java.net.HttpURLConnection conn =
                            (java.net.HttpURLConnection) new java.net.URL(url).openConnection();

                    conn.setRequestMethod("PUT");
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Content-Type", "application/json");

                    String body = "{ \"age\": " + (20 + id) + " }";

                    try (var os = conn.getOutputStream()) {
                        os.write(body.getBytes());
                    }

                    int code = conn.getResponseCode();
                    System.out.println("Thread " + id + " → " + code);

                } catch (Exception e) {
                    System.out.println("Thread " + id + " ERROR: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
