package com.concurrentdb.concurrent_db;


import com.concurrentdb.concurrent_db.Service.DatabaseService;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class ConcurrencyTest {

    private static final String BASE_URL = "http://localhost:8080/db/users/";

    @Test
    public void stressTest() throws Exception {

        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            int threadId = i;

            executor.submit(() -> {
                try {

                    // 🔥 Each thread works on SAME key → high contention
                    String key = "1";

                    // PUT
                    sendRequest("POST", BASE_URL + key,
                            "{\"name\":\"User" + threadId + "\",\"age\":" + (20 + threadId) + "}");

                    // UPDATE
                    sendRequest("PUT", BASE_URL + key,
                            "{\"age\":" + (30 + threadId) + "}");

                    // GET
                    sendRequest("GET", BASE_URL + key, null);

                    // DELETE (optional — comment if too aggressive)
                    // sendRequest("DELETE", BASE_URL + key, null);

                } catch (Exception e) {
                    System.out.println("Thread " + threadId + " ERROR: " + e.getMessage());
                }
            });
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
            Thread.sleep(100);
        }

        System.out.println("🔥 Stress test completed");
    }

    private void sendRequest(String method, String urlStr, String body) throws Exception {

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(method);
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);

        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes());
                os.flush();
            }
        }

        int responseCode = conn.getResponseCode();

        // Accept expected failures (due to concurrency)
        if (responseCode >= 500) {
            throw new RuntimeException("Server error: " + responseCode);
        }
    }
}