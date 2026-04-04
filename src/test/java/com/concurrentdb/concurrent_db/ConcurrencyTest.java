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

@SpringBootTest
public class ConcurrencyTest {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private TransactionManager transactionManager;

    @Test
    public void stressTestConcurrentTransactions() throws InterruptedException {

        int threads = 10;
        int tasks = 50;

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < tasks; i++) {
            int id = i;

            executor.submit(() -> {
                String txId = null;

                try {
                    // 🔥 BEGIN → auto generate txId
                    txId = transactionManager.begin();

                    // 🔥 DATA
                    Map<String, Object> data = new HashMap<>();
                    data.put("name", "User" + id);
                    data.put("age", 20 + id);

                    // 🔥 INSERT
                    databaseService.put("users", String.valueOf(id), data, txId);

                    // 🔥 COMMIT
                    databaseService.commit(txId);

                    System.out.println("✅ SUCCESS: " + txId);

                } catch (Exception e) {
                    System.out.println("❌ FAIL: " + txId + " → " + e.getMessage());

                    // rollback if needed
                    if (txId != null) {
                        try {
                            databaseService.rollback(txId);
                        } catch (Exception ignored) {}
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        System.out.println("🔥 STRESS TEST COMPLETED");
    }
}
