package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import com.concurrentdb.concurrent_db.lock.LockManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseService {
    private final InMemoryDatabase database;
    private final LockManager lockManager;

    public DatabaseService(InMemoryDatabase database, LockManager lockManager) {
        this.database = database;
        this.lockManager = lockManager;
    }
    public void put(String table, String key, Map<String,Object> data){
        validate(table, key, null);
        String lockKey = table + ":" + key;
        var lock = lockManager.getLock(lockKey).writeLock();
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

    public Row get(String table, String key){
        validate(table, key, null);
        String lockKey = table + ":" + key;
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

    public void delete(String table, String key) {
        validate(table, key, null);
        String lockKey = table + ":" + key;
        var lock = lockManager.getLock(lockKey).writeLock();
        lock.lock();
        try{
            Row row = database.getTable(table).getRow(key);

            if (row == null) {
                throw new RuntimeException("Row not found");
            }

            database.getTable(table).deleteRow(key);
        }finally {
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

}
