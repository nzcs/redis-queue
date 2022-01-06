package com.nzcs.queue.service;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class RedisLockService {

    final static int LOCK_WAIT_TIME = 5;
    final static int LOCK_AT_MOST_UNTIL = 10;
    RedissonClient redisson = Redisson.create();
    final Map<String, RLock> locks = new ConcurrentHashMap<>();


    boolean lock(String id) {
        RLock lock = this.redisson.getLock(id);
        boolean result = false;
        try {
//            System.out.printf("Try to lock sm with id %s%n", id);
            result = lock.tryLock(LOCK_WAIT_TIME, LOCK_AT_MOST_UNTIL, TimeUnit.SECONDS);
            this.locks.put(id, lock);
//            System.out.printf("Lock sm with id %s: %s%n", id, result);
        } catch (InterruptedException e) {
            System.out.printf("Cannot acquire lock for state machine with id %s%n", id);
        }
        return result;
    }

    void unLock(String id) {
        RLock lock = locks.remove(id);
        if (lock != null) {
//            System.out.printf("Unlock on %s%n", id);
            lock.unlock();
        }
    }
}
