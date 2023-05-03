package com.example.kafkaproducer.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@Slf4j
@RequestMapping("/api/kafka")
public class ProducerController {

    private final KafkaTemplate kafkaTemplate;

    private static final String _TOPIC_TEST = "test";

    @Autowired
    public ProducerController(
            KafkaTemplate kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send-msg")
    public void sendMsg(@RequestBody String message) {
        kafkaTemplate.send(_TOPIC_TEST, message);
    }

    @PostMapping("/send-msg-blocking")
    public void sendMsgBlocking(@RequestBody String message) {
        CompletableFuture<SendResult<String, String>> cf = kafkaTemplate.send(_TOPIC_TEST, message).completable();
        cf.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                log.info("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

}
