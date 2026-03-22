package com.concurrentdb.concurrent_db.Controller;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Service.DatabaseService;
import com.concurrentdb.concurrent_db.Transactions.TransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/db")
public class DatabaseController {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private TransactionManager transactionManager

    @PostMapping("/{table}/{key}")
    public void insert(@PathVariable String table,
                       @PathVariable String key,
                       @RequestBody Map<String, Object> data){
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }

        if (key == null || key.isEmpty()) {
            throw new RuntimeException("Key cannot be empty");
        }

        if(data == null || data.isEmpty()){
            throw new RuntimeException("Request body cannot be empty");
        }
        databaseService.put(table,key,data);
    }
    @GetMapping("/{table}/{key}")
    public Row get(@PathVariable String table,
                   @PathVariable String key){
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        if (key == null || key.isEmpty()) {
            throw new RuntimeException("Key cannot be empty");
        }
        return databaseService.get(table,key);
    }

    @DeleteMapping("/{table}/{key}")
    public void delete(@PathVariable String table,
                       @PathVariable String key) {
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("Table name cannot be empty");
        }
        if (key == null || key.isEmpty()) {
            throw new RuntimeException("Key cannot be empty");
        }
        databaseService.delete(table, key);
    }

    @PostMapping("/tx/begin")
    public void beginTransaction(){
        transactionManager.begin();
    }
    @PostMapping("/tx/commit")
    public void commitTransaction() {
        databaseService.commit();
    }

    @PostMapping("/tx/rollback")
    public void rollbackTransaction() {
        databaseService.rollback();
    }

}
