package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DatabaseService {
    private final InMemoryDatabase database = new InMemoryDatabase();

    public void put(String table, String key, Map<String,Object> data){
        Row row = new Row(key, data);
        database.getTable(table).putRow(key,row);
    }

    public Row get(String table, String key){
        return database.getTable(table).getRow(key);
    }

}
