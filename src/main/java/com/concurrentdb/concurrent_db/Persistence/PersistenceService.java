package com.concurrentdb.concurrent_db.Persistence;

import com.concurrentdb.concurrent_db.Storage.InMemoryDatabase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class PersistenceService {
    private static final String FILE_PATH = "db.json";
    private final ObjectMapper mapper = new ObjectMapper();
    public void save(InMemoryDatabase database){
        try {
            mapper.writeValue(new File(FILE_PATH),database);
        }catch (Exception e){
            throw new RuntimeException("Error Saving Database",e);
        }
    }
    public InMemoryDatabase load(){
        try {
            File file = new File(FILE_PATH);
            if(!file.exists()){
                return new InMemoryDatabase();
            }
            return mapper.readValue(file,InMemoryDatabase.class);

        }catch (Exception e){
            throw new RuntimeException("Error Loading Database",e);
        }
    }
}
