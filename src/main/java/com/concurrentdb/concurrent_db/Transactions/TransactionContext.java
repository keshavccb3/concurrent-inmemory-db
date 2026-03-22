package com.concurrentdb.concurrent_db.Transactions;


import java.util.HashMap;
import java.util.Map;

public class TransactionContext {
    private final Map<String,Object> changes = new HashMap<>();

    public void put(String key, Object data){
        changes.put(key,data);
    }

    public Map<String,Object> getChanges(){
        return changes;
    }

}
