package com.concurrentdb.concurrent_db.Model;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

@Data
public class Table {
    private ConcurrentHashMap<String, Row> rows = new ConcurrentHashMap<>();
    public Table(){}
    public void putRow(String key, Row row){
        rows.put(key, row);
    }
    public Row getRow(String key){
        return rows.get(key);
    }

    public void deleteRow(String key){
        rows.remove(key);
    }
}
