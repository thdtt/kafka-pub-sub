package com.example.kafkaconsumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerListener {

    private static final String _TOPIC_TEST = "test";
    private static final String _GROUP_ID = "foo";

    @KafkaListener(topics = _TOPIC_TEST, groupId = _GROUP_ID)
    public void listenGroupFoo(String message) {
        log.info("\tReceived Message in group foo: {}\n", message);
    }

}
