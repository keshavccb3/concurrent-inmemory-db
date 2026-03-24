package com.concurrentdb.concurrent_db.Transactions;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TransactionManager {

    private final Map<String, TransactionContext> transactions = new ConcurrentHashMap<>();

    public String begin() {
        String txId = UUID.randomUUID().toString();
        transactions.put(txId, new TransactionContext());
        return txId;
    }

    public TransactionContext getContext(String txId) {
        return transactions.get(txId);
    }

    public void remove(String txId) {
        transactions.remove(txId);
    }
}
