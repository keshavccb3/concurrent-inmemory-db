package com.concurrentdb.concurrent_db.Model;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Table {

    private Map<String, Row> rows = new ConcurrentHashMap<>();

    // column → value → set of keys
    private Map<String, Map<Object, Set<String>>> indexes = new ConcurrentHashMap<>();

    public Table() {}

    public Row getRow(String key) {
        return rows.get(key);
    }

    public void putRow(String key, Row row) {

        if (key == null) {
            throw new RuntimeException("FATAL: key is NULL in putRow");
        }

        if (row == null) {
            throw new RuntimeException("FATAL: row is NULL");
        }

        if (row.getKey() == null) {
            throw new RuntimeException("FATAL: row.key is NULL");
        }

        Row old = rows.get(key);

        if (old != null) {
            removeFromIndexes(key, old);
        }

        rows.put(key, row);

        updateIndexes(key, row);
    }

    public void deleteRow(String key) {

        if (key == null) return;

        Row old = rows.get(key);

        if (old != null) {
            removeFromIndexes(key, old);
        }

        rows.remove(key);
    }

    public void updateIndexes(String key, Row row) {

        if (key == null || row == null || row.getData() == null) return;

        for (var entry : row.getData().entrySet()) {

            String column = entry.getKey();
            Object value = entry.getValue();
            if (column == null || value == null) {
                continue;
            }

            Map<Object, Set<String>> valueMap = indexes.get(column);

            if (valueMap == null) {
                valueMap = new ConcurrentHashMap<>();
                indexes.put(column, valueMap);
            }

            Set<String> keys = valueMap.get(value);

            if (keys == null) {
                keys = ConcurrentHashMap.newKeySet();
                valueMap.put(value, keys);
            }

            keys.add(key);
        }
    }

    public void removeFromIndexes(String key, Row row) {

        try {

            if (key == null) {
                throw new RuntimeException("REMOVE: key is NULL");
            }

            if (row == null || row.getData() == null) return;

            for (var entry : row.getData().entrySet()) {

                String column = entry.getKey();
                Object value = entry.getValue();

                if (column == null || value == null) continue;

                Map<Object, Set<String>> valueMap = indexes.get(column);
                if (valueMap == null) continue;

                Set<String> keys = valueMap.get(value);
                if (keys == null) continue;

                keys.remove(key);

                if (keys.isEmpty()) {
                    valueMap.remove(value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
