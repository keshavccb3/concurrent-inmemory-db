package com.concurrentdb.concurrent_db.Transactions;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class TransactionContext {
    private final Map<String,Object> changes = new HashMap<>();
    private final Map<String,Integer> originalVersions = new HashMap<>();


    public void put(String key, Object data){

        if (key == null || key.isEmpty()) {
            throw new RuntimeException("Transaction key is null or empty ");
        }

        changes.put(key, data);
    }

    public Map<String,Object> getChanges(){
        return changes;
    }

    public void setOriginalVersion(String key, int version){

        if (key == null) {
            throw new RuntimeException("Version key is null");
        }

        originalVersions.put(key, version);
    }

    public int getOriginalVersion(String key){
        return originalVersions.getOrDefault(key, 0);
    }




}
