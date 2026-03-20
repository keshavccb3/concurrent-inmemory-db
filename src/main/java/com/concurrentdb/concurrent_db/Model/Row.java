package com.concurrentdb.concurrent_db.Model;

import java.util.*;

public class Row {
    private String key;
    private Map<String,Object> data;
    public Row(String key, Map<String,Object> data){
        this.key = key;
        this.data = data;
    }

    public String getKey(){
        return key;
    }
    public Map<String,Object> getData(){
        return data;
    }
}
