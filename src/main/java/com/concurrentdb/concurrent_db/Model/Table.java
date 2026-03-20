package com.concurrentdb.concurrent_db.Model;

import java.util.concurrent.ConcurrentHashMap;

public class Table {
    private ConcurrentHashMap<String, Row> rows = new ConcurrentHashMap<>();
    public void putRow(String key, Row row){
        rows.put(key, row);
    }
    public Row getRow(String key){
        return rows.get(key);
    }

    public void removeRow(String key){
        rows.remove(key);
        return;
    }
}
