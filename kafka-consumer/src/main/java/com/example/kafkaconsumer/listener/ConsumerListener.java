package com.example.kafkaconsumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.net.SocketTimeoutException;
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
    @KafkaListener(topics = _TOPIC_TEST_1, groupId = _GROUP_ID, concurrency = "5")
    public void listenGroupFoo(String message) {
        log.info("\tReceived Message thread {} in group foo: {}\n", Thread.currentThread().getId(), message);
    }

    /**
     * Multiple Topic handler
     * @param message
     */
//    @KafkaListener(topics = "test, test-2", groupId = _GROUP_ID)
//    public void multiTopicListenGroupFoo(String message) {
//        log.info("\tReceived Message in group foo: {}\n", message);
//    }

    @KafkaListener(topics = "topicName")
    @KafkaHandler
    @RetryableTopic(
            backoff = @Backoff(value = 3000L),
            attempts = "5",
            autoCreateTopics = "false",
            include = SocketTimeoutException.class, exclude = NullPointerException.class)
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println(
                "Received Message: " + message
                        + "from partition: " + partition);
        if(message.equals("error"))
            throw new RuntimeException("buh");
    }



}
