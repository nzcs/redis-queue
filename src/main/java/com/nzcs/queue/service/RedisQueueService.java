package com.nzcs.queue.service;

import org.redisson.Redisson;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.InitializingBean;

import java.util.Collections;
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
        RDelayedQueue<Object> delayedQueue = redisson.getDelayedQueue(queue);

        queue.subscribeOnElements(value -> {
            Thread.currentThread().setName(sign + "_" + Thread.currentThread().getId());
            String id = value.toString().substring(0, 3);

            if (!lockService.lock(id)) {
//                System.out.printf("Again(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);
                delayedQueue.offerAsync(value, 300, TimeUnit.MILLISECONDS);

            } else {
                System.out.printf("Start(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);
                delayedQueue.offerAsync(value, 300, TimeUnit.MILLISECONDS);

                try {
                    Thread.sleep(1000);
                    dummyStore.add(value);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                delayedQueue.removeAll(Collections.singleton(value));
                queue.removeAll(Collections.singleton(value));
//                System.out.printf("Remove(%s) %s: %s%n", sign, queue.removeAll(Collections.singleton(value)), value);
                lockService.unLock(id);

                System.out.printf("End(%s) %s: %s%n", sign, Thread.currentThread().getId(), value);
            }
        });

    }
}
