package com.iceblcok.springboot.kafka;

import com.iceblcok.springboot.kafka.producer.KafkaSenderService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaApplicationTests {

    @Test
    public void contextLoads() {

    }

    @Autowired
    private KafkaSenderService senderService;

    /**
     * 测试发送消息到 Kafka，消息会被 KafkaReceiverListener 接收
     *
     * @throws IOException
     */
    @Test
    public void sendKafka() throws IOException, InterruptedException {
        // 发送消息数量
        int num = 10000000;
        // topic 与 KafkaReceiverListener 中 topics 相对应
        String topic = "iov-topic";
        for (int i = 0; i < num; i++) {
            senderService.sendMessage(topic, new Date() + "_" + i);
        }

        // 阻塞，使消息可以被监听接收
        System.in.read();
    }

}
