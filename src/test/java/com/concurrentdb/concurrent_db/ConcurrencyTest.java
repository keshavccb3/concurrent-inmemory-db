package com.concurrentdb.concurrent_db;


import com.concurrentdb.concurrent_db.Service.DatabaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest
public class ConcurrencyTest {

    @Autowired
    private DatabaseService service;

    @Test
    void testConcurrentInsert() throws InterruptedException {

        int threadCount = 10;

        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {

            threads[i] = new Thread(() -> {
                try {
                    Map<String, Object> data = new HashMap<>();
                    data.put("name", "User");

                    service.put("users", "101", data);

                    System.out.println(Thread.currentThread().getName() + " SUCCESS");

                } catch (Exception e) {
                    System.out.println(Thread.currentThread().getName() + " FAILED: " + e.getMessage());
                }
            });
        }

        // start all threads
        for (Thread t : threads) {
            t.start();
        }

        // wait for all to finish
        for (Thread t : threads) {
            t.join();
        }
    }
}
