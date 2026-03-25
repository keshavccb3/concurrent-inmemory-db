package com.concurrentdb.concurrent_db.Model;


import lombok.*;

import java.util.*;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Row {
    private String key;
    private Map<String,Object> data;
    private int version;
    public Row(String key, Map<String,Object> data){
        this.key = key;
        this.data = data;
        this.version = 0;
    }

}
