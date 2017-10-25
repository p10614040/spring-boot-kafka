package com.iceblcok.springboot.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 消息消费监听
 *
 * @author yan.liang@ustcinfo.com
 * @date 2017/6/9
 */
@Component
public class KafkaReceiverListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiverListener.class);

    @KafkaListener(topics = "test")
    public void processMessage(String message) {
        LOGGER.info("receive topic='{}' message='{}'", "test", message);
    }
}
