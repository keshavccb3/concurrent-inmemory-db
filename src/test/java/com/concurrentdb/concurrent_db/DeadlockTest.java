package com.concurrentdb.concurrent_db;
import com.concurrentdb.concurrent_db.lock.LockManager;

import java.util.concurrent.TimeUnit;


public class DeadlockTest {

    public static void main(String[] args) {

        LockManager lockManager = new LockManager();

        Runnable t1 = () -> {
            var lock1 = lockManager.getLock("users:1").writeLock();
            var lock2 = lockManager.getLock("users:2").writeLock();

            boolean l1 = false, l2 = false;

            try {
                l1 = lock1.tryLock(2, TimeUnit.SECONDS);
                if (!l1) throw new RuntimeException("T1 failed lock1");

                System.out.println("T1 locked user1");

                Thread.sleep(200);

                l2 = lock2.tryLock(2, TimeUnit.SECONDS);
                if (!l2) throw new RuntimeException("T1 deadlock on user2");

                System.out.println("T1 locked user2");

            } catch (Exception e) {
                System.out.println("T1 ERROR: " + e.getMessage());
            } finally {
                if (l2) lock2.unlock();
                if (l1) lock1.unlock();
            }
        };

        Runnable t2 = () -> {
            var lock1 = lockManager.getLock("users:1").writeLock();
            var lock2 = lockManager.getLock("users:2").writeLock();

            boolean l1 = false, l2 = false;

            try {
                l2 = lock2.tryLock(2, TimeUnit.SECONDS);
                if (!l2) throw new RuntimeException("T2 failed lock2");

                System.out.println("T2 locked user2");

                Thread.sleep(200);

                l1 = lock1.tryLock(2, TimeUnit.SECONDS);
                if (!l1) throw new RuntimeException("T2 deadlock on user1");

                System.out.println("T2 locked user1");

            } catch (Exception e) {
                System.out.println("T2 ERROR: " + e.getMessage());
            } finally {
                if (l1) lock1.unlock();
                if (l2) lock2.unlock();
            }
        };

        new Thread(t1).start();
        new Thread(t2).start();
    }
}
