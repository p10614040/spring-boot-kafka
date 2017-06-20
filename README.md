# SpringBoot Kafka
基于 SpringBoot 的 Kafka 简单生产、消费程序，演示如何搭建 Kafka 客户端。

* JDK：1.8+
* SpringBoot：1.5.4.RELEASE
* spring-kafka：1.0.6.RELEASE（与 Apache Kafka 版本相对应，否则无法访问）
* Apache Kafka：2.9.2-0.8.1.1

## 生产者
消息发送使用多线程防止消息阻塞，可以使用同步或者异步方式发送消息。

## 消费者
使用 KafkaListener 监听 Topic 实时获取消息，Demo 中只打印消息。
