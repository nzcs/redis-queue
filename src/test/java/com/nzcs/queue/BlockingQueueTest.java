package com.nzcs.queue;

import com.nzcs.queue.service.RedisQueueService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;


public class BlockingQueueTest {

    RedissonClient redisson = Redisson.create();
    RedisQueueService service = new RedisQueueService();


    @BeforeEach
    public void before() throws Exception {
        service.afterPropertiesSet();
        redisson.getKeys().flushdb();
    }

    @AfterEach
    public void after() {
        redisson.getKeys().flushdb();
    }


    @Test
    public void test() throws InterruptedException {

        service.put("a", "aaa");
        service.put("a", "aaa_x");
        service.put("b", "bbb");
        service.put("b", "bbb_x");
        service.put("c", "ccc");
        service.put("c", "ccc_x");
        service.put("d", "ddd");
        service.put("d", "ddd_x");

        Thread.sleep(5000);
    }
}
