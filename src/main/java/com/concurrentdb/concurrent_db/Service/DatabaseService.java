package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Model.Table;
import com.concurrentdb.concurrent_db.Persistence.PersistenceService;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import com.concurrentdb.concurrent_db.Transactions.TransactionContext;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import com.concurrentdb.concurrent_db.lock.LockManager;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseService {
    private final InMemoryDatabase database;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private final PersistenceService persistenceService;

    public DatabaseService(InMemoryDatabase database,
                           LockManager lockManager,
                           TransactionManager transactionManager,
                           PersistenceService persistenceService) {
        this.database = database;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.persistenceService = persistenceService;
    }

    // ================= PUT =================
    public void put(String table, String key, Map<String, Object> data, String txId) {

        validate(table, key, data);
        String lockKey = table + ":" + key;

        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;

        if (tx != null) {
            Row existing = database.getTable(table).getRow(key);
            int version = (existing == null) ? 0 : existing.getVersion();
            tx.setOriginalVersion(lockKey, version);
            tx.put(lockKey, new Row(key, data));
            return;
        }

        var lock = lockManager.getLock(lockKey).writeLock();
        boolean acquired = false;

        try {
            acquired = lock.tryLock(2, TimeUnit.SECONDS);

            if (!acquired) throw new RuntimeException("Deadlock detected (PUT)");

            if (database.getTable(table).getRow(key) != null) {
                throw new RuntimeException("Key already exists");
            }

            Row row = new Row(key, data);
            database.getTable(table).putRow(key, row);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted");
        } finally {
            if (acquired) lock.unlock();
        }
    }

    // ================= GET =================
    public Row get(String table, String key, String txId) {

        validate(table, key, null);
        String lockKey = table + ":" + key;

        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;

        if (tx != null && tx.getChanges().containsKey(lockKey)) {
            return (Row) tx.getChanges().get(lockKey);
        }

        var lock = lockManager.getLock(lockKey).readLock();
        boolean acquired = false;

        try {
            acquired = lock.tryLock(2, TimeUnit.SECONDS);

            if (!acquired) throw new RuntimeException("Deadlock detected (GET)");

            Row row = database.getTable(table).getRow(key);
            if (row == null) throw new RuntimeException("Row not found");

            return row;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted");
        } finally {
            if (acquired) lock.unlock();
        }
    }

    // ================= DELETE =================
    public void delete(String table, String key, String txId) {

        validate(table, key, null);
        String lockKey = table + ":" + key;

        TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;

        if (tx != null) {
            Row existing = database.getTable(table).getRow(key);
            if (existing == null) throw new RuntimeException("Row not found");

            tx.setOriginalVersion(lockKey, existing.getVersion());
            tx.put(lockKey, null);
            return;
        }

        var lock = lockManager.getLock(lockKey).writeLock();
        boolean acquired = false;

        try {
            acquired = lock.tryLock(2, TimeUnit.SECONDS);

            if (!acquired) throw new RuntimeException("Deadlock detected (DELETE)");

            Row row = database.getTable(table).getRow(key);
            if (row == null) throw new RuntimeException("Row not found");

            database.getTable(table).deleteRow(key);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted");
        } finally {
            if (acquired) lock.unlock();
        }
    }

    // ================= UPDATE =================
    public void update(String table, String key, Map<String, Object> data, String txId) {

        validate(table, key, data);
        String lockKey = table + ":" + key;

        var lock = lockManager.getLock(lockKey).writeLock();
        boolean acquired = false;

        try {
            acquired = lock.tryLock(2, TimeUnit.SECONDS);

            if (!acquired) throw new RuntimeException("Deadlock detected (UPDATE)");

            Row existing = database.getTable(table).getRow(key);
            if (existing == null) throw new RuntimeException("Row does not exist");

            Map<String, Object> updatedData = new HashMap<>(existing.getData());

            for (var entry : data.entrySet()) {
                if (entry.getValue() != null) {
                    updatedData.put(entry.getKey(), entry.getValue());
                }
            }

            Row newRow = new Row(key, updatedData);

            TransactionContext tx = (txId != null) ? transactionManager.getContext(txId) : null;

            if (tx != null) {
                tx.put(lockKey, newRow);
                tx.setOriginalVersion(lockKey, existing.getVersion());
                return;
            }

            newRow.setVersion(existing.getVersion() + 1);
            database.getTable(table).putRow(key, newRow);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted");
        } finally {
            if (acquired) lock.unlock();
        }
    }

    // ================= COMMIT =================
    public void commit(String txId) {

        TransactionContext tx = transactionManager.getContext(txId);
        if (tx == null) throw new RuntimeException("No active transaction");

        for (var entry : tx.getChanges().entrySet()) {

            String lockKey = entry.getKey();
            String[] parts = lockKey.split(":");
            String table = parts[0];
            String key = parts[1];

            var lock = lockManager.getLock(lockKey).writeLock();
            boolean acquired = false;

            try {
                acquired = lock.tryLock(2, TimeUnit.SECONDS);

                if (!acquired) throw new RuntimeException("Deadlock detected (COMMIT)");

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

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted");
            } finally {
                if (acquired) lock.unlock();
            }
        }

        transactionManager.remove(txId);
        persistenceService.save(database);
    }

    // ================= ROLLBACK =================
    public void rollback(String txId) {
        if (transactionManager.getContext(txId) == null) {
            throw new RuntimeException("No active transaction");
        }
        transactionManager.remove(txId);
    }

    // ================= CLEAR =================
    public void clear() {
        database.clear();
        persistenceService.clear();
    }

    // ================= VALIDATION =================
    private void validate(String table, String key, Map<String, Object> data) {

        if (table == null || table.isEmpty())
            throw new RuntimeException("Table name cannot be empty");

        if (key == null || key.isEmpty())
            throw new RuntimeException("Key cannot be empty");

        if (data != null && data.isEmpty())
            throw new RuntimeException("Request body cannot be empty");
    }

    public List<Row> search(String table, String colunm, String value){
        if(table == null || colunm == null || value == null){
            throw new RuntimeException("Invalid parameters");
        }
        Table t =  database.getTable(table);
        Map<Object, Set<String>> valueMap = t.getIndexes().get(colunm);
        if(valueMap == null){
            return new ArrayList<>();
        }
        Set<String> keys = valueMap.get(value);
        if(keys == null){
            return new ArrayList<>();
        }
        List<Row> result = new ArrayList<>();
        for(String key : keys){
            Row row = t.getRow(key);
            if(row!=null){
                result.add(row);
            }
        }
        return result;

    }

    public List<Row> searchMulti(String table, Map<String,String> filters){
        if(table == null || filters.isEmpty()){
            throw new RuntimeException("Invalid parameters");
        }
        Table t =  database.getTable(table);
        Set<String> resultKeys = null;
        for(val entry : filters.entrySet()){
            String colunm  = entry.getKey();
            String value  = entry.getValue();
            Map<Object,Set<String>> valueMap = t.getIndexes().get(colunm);
            if(valueMap == null){
                return new ArrayList<>();
            }
            Set<String> keys = valueMap.get(value);
            if(keys == null){
                return new ArrayList<>();
            }
            if(resultKeys == null){
                resultKeys = new HashSet<>(keys);
            }else{
                resultKeys.retainAll(keys);
            }
        }
        List<Row> result = new ArrayList<>();
        if(resultKeys != null){
            for(String key : resultKeys){
                Row row = t.getRow(key);
                if(row!=null){
                    result.add(row);
                }
            }
        }
        return result;
    }

    public List<Row> searchOr(String table, MultiValueMap<String, String> filters) {

        Table t = database.getTable(table);

        Set<String> resultKeys = new HashSet<>();

        for (var entry : filters.entrySet()) {

            String column = entry.getKey();
            List<String> values = entry.getValue();

            Map<Object, Set<String>> valueMap = t.getIndexes().get(column);

            if (valueMap == null) continue;

            for (String value : values) {

                Set<String> keys = valueMap.get(value);

                if (keys != null) {
                    resultKeys.addAll(keys);
                }
            }
        }

        List<Row> result = new ArrayList<>();

        for (String key : resultKeys) {
            Row row = t.getRow(key);
            if (row != null) {
                result.add(row);
            }
        }

        return result;
    }

    public List<Row> searchRange(String table, String column, String op, String value) {

        Table t = database.getTable(table);

        TreeMap<Object, Set<String>> valueMap = t.getIndexes().get(column);

        if (valueMap == null || valueMap.isEmpty()) {
            return new ArrayList<>();
        }

        Object sampleKey = valueMap.firstKey();

        Object parsedValue = parseValue(sampleKey, value);

        NavigableMap<Object, Set<String>> subMap;

        switch (op) {
            case ">":
                subMap = valueMap.tailMap(parsedValue, false);
                break;
            case ">=":
                subMap = valueMap.tailMap(parsedValue, true);
                break;
            case "<":
                subMap = valueMap.headMap(parsedValue, false);
                break;
            case "<=":
                subMap = valueMap.headMap(parsedValue, true);
                break;
            default:
                throw new RuntimeException("Invalid operator");
        }

        Set<String> resultKeys = new HashSet<>();

        for (Set<String> keys : subMap.values()) {
            resultKeys.addAll(keys);
        }

        List<Row> result = new ArrayList<>();

        for (String key : resultKeys) {
            Row row = t.getRow(key);
            if (row != null) {
                result.add(row);
            }
        }

        return result;
    }

    private Object parseValue(Object sample, String value) {

        if (sample instanceof Integer) {
            return Integer.parseInt(value);
        }

        if (sample instanceof Long) {
            return Long.parseLong(value);
        }

        if (sample instanceof Double) {
            return Double.parseDouble(value);
        }

        return value;
    }

    public List<Row> searchRange(String table, String column, String op, String value,
                                 int page, int size, String sort, String order) {

        List<Row> result = searchRange(table, column, op, value);

        if (sort != null) {
            result.sort((r1, r2) -> {

                Object v1 = r1.getData().get(sort);
                Object v2 = r2.getData().get(sort);

                if (v1 == null || v2 == null) return 0;

                Comparable c1 = (Comparable) v1;
                Comparable c2 = (Comparable) v2;

                return order.equalsIgnoreCase("desc")
                        ? c2.compareTo(c1)
                        : c1.compareTo(c2);
            });
        }

        int start = page * size;
        int end = Math.min(start + size, result.size());

        if (start >= result.size()) {
            return new ArrayList<>();
        }

        return result.subList(start, end);
    }

}
