package com.concurrentdb.concurrent_db.Controller;

import com.concurrentdb.concurrent_db.Model.Row;
import com.concurrentdb.concurrent_db.Service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/db")
public class DatabaseController {

    @Autowired
    private DatabaseService databaseService;

    @PostMapping("/{table}/{key}")
    public void insert(@PathVariable String table,
                       @PathVariable String key,
                       @RequestBody Map<String, Object> data){
        databaseService.put(table,key,data);
    }
    @GetMapping("/{table}/{key}")
    public Row get(@PathVariable String table,
                   @PathVariable String key){
        return databaseService.get(table,key);
    }

}
