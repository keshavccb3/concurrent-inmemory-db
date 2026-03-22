package com.concurrentdb.concurrent_db.Storage;

import com.concurrentdb.concurrent_db.Model.Table;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryDatabase {
    private ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();
    public Table getTable(String name){
        return tables.computeIfAbsent(name,k->new Table());
    }
}
