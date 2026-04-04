package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Persistence.PersistenceService;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import com.concurrentdb.concurrent_db.Transactions.TransactionContext;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import com.concurrentdb.concurrent_db.lock.LockManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseService {
    private final InMemoryDatabase database;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private final PersistenceService persistenceService;

    public DatabaseService(InMemoryDatabase database, LockManager lockManager, TransactionManager transactionManager, PersistenceService persistenceService) {
        this.database = database;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.persistenceService = persistenceService;
    }
    public void put(String table, String key, Map<String,Object> data, String txId){
        validate(table, key, null);
        String lockKey = table + ":" + key;
        var lock = lockManager.getLock(lockKey).writeLock();
        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;
        if(tx!=null){
            Row existing = database.getTable(table).getRow(key);

            int version = (existing == null) ? 0 : existing.getVersion();

            tx.setOriginalVersion(lockKey, version);
            tx.put(lockKey, new Row(key,data));
            return;
        }
        // Normal flow
        lock.lock();
        try {
            if(database.getTable(table).getRow(key)!=null){
                throw new RuntimeException("key already exists");
            }
            Row row = new Row(key, data);
            database.getTable(table).putRow(key,row);
        }finally {
            lock.unlock();
        }

    }

    public Row get(String table, String key, String txId){
        validate(table, key, null);
        String lockKey = table + ":" + key;
        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;
        if (tx != null && tx.getChanges().containsKey(lockKey)) {
            return (Row) tx.getChanges().get(lockKey);
        }
        var lock = lockManager.getLock(lockKey).readLock();
        lock.lock();
        try {
            Row row = database.getTable(table).getRow(key);
            if(row == null){
                throw new RuntimeException("Row not found");
            }
            return row;
        }finally {
            lock.unlock();
        }

    }

    public void delete(String table, String key, String txId){

        validate(table, key, null);

        String lockKey = table + ":" + key;
        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;

        if (tx != null) {
            Row existing = database.getTable(table).getRow(key);
            if (existing == null) {
                throw new RuntimeException("Row not found");
            }
            tx.setOriginalVersion(lockKey, existing.getVersion());
            tx.put(lockKey, null);
            return;
        }

        var lock = lockManager.getLock(lockKey).writeLock();

        lock.lock();
        try {
            Row row = database.getTable(table).getRow(key);

            if (row == null) {
                throw new RuntimeException("Row not found");
            }

            database.getTable(table).deleteRow(key);
        } finally {
            lock.unlock();
        }
    }

    public void update(String table, String key, Map<String, Object> data, String txId) {

        validate(table, key, data);
        String lockKey = table + ":" + key;
        var lock = lockManager.getLock(lockKey).writeLock();

        lock.lock();
        try {

            Row existing = database.getTable(table).getRow(key);

            if (existing == null) {
                throw new RuntimeException("Row does not exist");
            }

            Map<String, Object> updatedData = new HashMap<>(existing.getData());

            for (var entry : data.entrySet()) {
                if (entry.getValue() != null) {
                    updatedData.put(entry.getKey(), entry.getValue());
                }
            }

            Row newRow = new Row(key, updatedData);

            TransactionContext tx = transactionManager.getContext(txId);

            if (tx != null) {
                tx.put(lockKey, newRow);
                tx.setOriginalVersion(lockKey, existing.getVersion());
                return;
            }

            newRow.setVersion(existing.getVersion() + 1);
            database.getTable(table).putRow(key, newRow);

        } finally {
            lock.unlock();
        }
    }

    private void validate(String table, String key, Map<String, Object> data) {

        if (table == null || table.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }

        if (key == null || key.isEmpty()) {
            throw new RuntimeException("Key cannot be empty");
        }

        if (data != null && data.isEmpty()) {
            throw new RuntimeException("Request body cannot be empty");
        }
    }

    public void commit(String txId){

        TransactionContext tx = transactionManager.getContext(txId);

        if(tx == null){
            throw new RuntimeException("No active transaction");
        }

        for(var entry : tx.getChanges().entrySet()){

            String lockKey = entry.getKey();

            if (lockKey == null) {
                throw new RuntimeException("lockKey is null ");
            }

            if (!lockKey.contains(":")) {
                throw new RuntimeException("Invalid lockKey format: " + lockKey);
            }

            String[] parts = lockKey.split(":");

            if (parts.length != 2) {
                throw new RuntimeException("Invalid lockKey split: " + lockKey);
            }

            String table = parts[0];
            String key = parts[1];
            if (key == null || key.isEmpty()) {
                throw new RuntimeException("Parsed key is null from " + lockKey);
            }
            var lock = lockManager.getLock(lockKey).writeLock();

            lock.lock();
            try {

                Row current = database.getTable(table).getRow(key);

                int currentVersion = (current == null) ? 0 : current.getVersion();
                int originalVersion = tx.getOriginalVersion(lockKey);

                if (currentVersion != originalVersion) {
                    throw new RuntimeException("Conflict detected! Retry transaction.");
                }

                Object value = entry.getValue();

                if (value == null) {
                    database.getTable(table).deleteRow(key);
                } else {
                    Row newRow = (Row) value;
                    newRow.setVersion(currentVersion + 1);
                    database.getTable(table).putRow(key, newRow);
                }

            } finally {
                lock.unlock();
            }
        }

        transactionManager.remove(txId);
        persistenceService.save(database);
    }

    public void rollback(String txId) {
        TransactionContext tx = transactionManager.getContext(txId);

        if (tx == null) {
            throw new RuntimeException("No active transaction");
        }
        transactionManager.remove(txId);
    }

    public void clear() {
        database.clear();   // clear in-memory data
        persistenceService.clear(); // clear file (VERY IMPORTANT)
    }



}
