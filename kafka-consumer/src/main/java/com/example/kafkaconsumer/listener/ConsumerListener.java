package com.example.kafkaconsumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
@Slf4j
public class ConsumerListener {

    private static final String _TOPIC_TEST_1 = "test";
    private static final String _TOPIC_TEST_2 = "test-2";
    private static final String _GROUP_ID = "foo";

    /**
     * Single Topic handler
     * @param message
     */
    @KafkaListener(topics = _TOPIC_TEST_1, groupId = _GROUP_ID)
    public void listenGroupFoo(String message) {
        log.info("\tReceived Message in group foo: {}\n", message);
    }

    /**
     * Multiple Topic handler
     * @param message
     */
    @KafkaListener(topics = "test, test2", groupId = _GROUP_ID)
    public void multiTopicListenGroupFoo(String message) {
        log.info("\tReceived Message in group foo: {}\n", message);
    }

    @KafkaListener(topics = "topicName")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message: " + message
                        + "from partition: " + partition);
    }

}
