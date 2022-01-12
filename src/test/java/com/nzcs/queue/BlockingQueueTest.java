package com.nzcs.queue;

import com.nzcs.queue.service.RedisQueueService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

import static com.nzcs.queue.service.RedisQueueService.dummyStore;


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
    public void test() {

        service.put("b", "bbb");
        service.put("b", "bbb_x");
        service.put("a", "aaa_y");
        service.put("a", "aaa_a");
        service.put("c", "ccc");
        service.put("c", "ccc_x");
        service.put("d", "ddd");
        service.put("d", "ddd_x");

        Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> dummyStore.size() == 8);
    }
}
