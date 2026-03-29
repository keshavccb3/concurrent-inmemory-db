package com.concurrentdb.concurrent_db.Storage;

import com.concurrentdb.concurrent_db.Model.Table;
import com.concurrentdb.concurrent_db.Persistence.PersistenceService;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
@Data
public class InMemoryDatabase {
    @Autowired
    private PersistenceService persistenceService;
    private ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();
    public InMemoryDatabase() {}
    public Table getTable(String name){
        return tables.computeIfAbsent(name,k->new Table());
    }

    @PostConstruct
    public void init(){
        InMemoryDatabase loaded = persistenceService.load();
        if(loaded!=null && loaded.getTables()!=null){
            this.tables = loaded.getTables();
        }
    }

}
