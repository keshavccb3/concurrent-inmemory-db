package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import com.concurrentdb.concurrent_db.Transactions.TransactionContext;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import com.concurrentdb.concurrent_db.lock.LockManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseService {
    private final InMemoryDatabase database;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;

    public DatabaseService(InMemoryDatabase database, LockManager lockManager, TransactionManager transactionManager) {
        this.database = database;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
    }
    public void put(String table, String key, Map<String,Object> data, String txId){
        validate(table, key, null);
        String lockKey = table + ":" + key;
        var lock = lockManager.getLock(lockKey).writeLock();
        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;
        if(tx!=null){
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
            Object value = entry.getValue();

            String[] parts = lockKey.split(":");
            String table = parts[0];
            String key = parts[1];

            var lock = lockManager.getLock(lockKey).writeLock();

            lock.lock();
            try {
                if (value == null) {
                    database.getTable(table).deleteRow(key);
                } else {
                    database.getTable(table).putRow(key, (Row) value);
                }
            } finally {
                lock.unlock();
            }
        }

        transactionManager.remove(txId);
    }

    public void rollback(String txId) {
        TransactionContext tx = transactionManager.getContext(txId);

        if (tx == null) {
            throw new RuntimeException("No active transaction");
        }
        transactionManager.remove(txId);
    }



}
