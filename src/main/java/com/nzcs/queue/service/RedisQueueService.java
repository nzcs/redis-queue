package com.nzcs.queue.service;

import org.redisson.Redisson;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class RedisQueueService implements InitializingBean {

    public static final Set<Object> dummyStore = new HashSet<>();

    public static final String QUEUE = "queue";
    RedissonClient redisson = Redisson.create();
    RedisLockService lockService = new RedisLockService();


    public void put(String key, Object value) {
        RBatch batch = redisson.createBatch(BatchOptions.defaults());
        batch.getBucket(key).setAsync(value);
        batch.getQueue(QUEUE).addAsync(value);
        batch.execute();
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        subscription("A");
        subscription("B");
        subscription("C");
    }

    private void subscription(String sign) {
        RBlockingQueue<Object> queue = redisson.getBlockingDeque(QUEUE);

        RBlockingQueue<Object> failedQueue = redisson.getBlockingDeque(QUEUE);
        RDelayedQueue<Object> delayedQueue = redisson.getDelayedQueue(failedQueue);

        queue.subscribeOnElements(value -> {
            Thread.currentThread().setName(sign + "_" + Thread.currentThread().getId());
            String id = value.toString().substring(0, 3);
            lockService.lock(id);

            System.out.printf("Start(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);

            delayedQueue.offer(value, 600, TimeUnit.MILLISECONDS);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            dummyStore.add(value);

            delayedQueue.remove(value);
            lockService.unLock(id);

            System.out.printf("End(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);
        });


        failedQueue.subscribeOnElements(value -> {
            Thread.currentThread().setName(sign + "_" + Thread.currentThread().getId());
            String id = value.toString().substring(0, 3);
            lockService.lock(id);

            System.out.printf("Start(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            dummyStore.add(value);

            lockService.unLock(id);

            System.out.printf("End(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);
        });
    }
}
