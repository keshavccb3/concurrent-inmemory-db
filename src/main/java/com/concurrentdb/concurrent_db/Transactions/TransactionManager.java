package com.concurrentdb.concurrent_db.Transactions;

import org.springframework.stereotype.Component;

@Component
public class TransactionManager {
    private final ThreadLocal<TransactionContext> context = new ThreadLocal<>();
    public void begin(){
        context.set(new TransactionContext());
    }
    public TransactionContext getContext(){
        return context.get();
    }
    public void clear(){
        context.remove();
    }
}
