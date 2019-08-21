package com.retry.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    private AtomicInteger i = new AtomicInteger();

    @KafkaListener(topics = "myHello11")
    public void consume(String message, Acknowledgment ack) throws Exception {
        System.out.println("receive:"+message);
        if("2".equals(message)){
            Thread.sleep(60000);
            throw new Exception("测试异常4343"+i.incrementAndGet());
        }

        ack.acknowledge();
    }


}
