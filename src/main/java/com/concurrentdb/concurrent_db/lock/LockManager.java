package com.concurrentdb.concurrent_db.lock;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class LockManager {
    private ConcurrentHashMap<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();

    public ReentrantReadWriteLock getLock(String key){
        return lockMap.computeIfAbsent(key,k->new ReentrantReadWriteLock());
    }
}
