package com.concurrentdb.concurrent_db.Service;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DatabaseService {
    private final InMemoryDatabase database = new InMemoryDatabase();

    public void put(String table, String key, Map<String,Object> data){


        if(database.getTable(table).getRow(key)!=null){
            throw new RuntimeException("key already exists");
        }
        Row row = new Row(key, data);
        database.getTable(table).putRow(key,row);
    }

    public Row get(String table, String key){

        Row row = database.getTable(table).getRow(key);
        if(row == null){
            throw new RuntimeException("Row not found");
        }
        return row;
    }

    public void delete(String table, String key) {
        Row row = database.getTable(table).getRow(key);

        if (row == null) {
            throw new RuntimeException("Row not found");
        }

        database.getTable(table).deleteRow(key);
    }

}
