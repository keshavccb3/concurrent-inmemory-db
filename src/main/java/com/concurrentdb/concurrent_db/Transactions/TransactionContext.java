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
        changes.put(key,data);
    }

    public Map<String,Object> getChanges(){
        return changes;
    }
    public void setOriginalVersion(String key, int version) {
        originalVersions.put(key, version);
    }

    public Integer getOriginalVersion(String key) {
        return originalVersions.get(key);
    }




}
