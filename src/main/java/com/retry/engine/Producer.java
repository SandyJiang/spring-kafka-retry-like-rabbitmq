package com.retry.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "myHello11";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        for(int i=5;i<100;i++){
            logger.info(String.format("#### -> Producing message -> %s", String.valueOf(i)));
            this.kafkaTemplate.send(TOPIC, i%5,null,String.valueOf(i));
            //this.kafkaTemplate.send(TOPIC,String.valueOf(i));
        }

    }
}
