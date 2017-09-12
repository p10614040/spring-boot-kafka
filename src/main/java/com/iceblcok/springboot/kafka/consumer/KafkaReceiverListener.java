package com.iceblcok.springboot.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka 消息消费监听
 *
 * @author yan.liang@ustcinfo.com
 * @date 2017/6/9
 */
@Component
public class KafkaReceiverListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiverListener.class);

    private static final String ZTE_A_TOPIC = "ZTE-A";
    private static final String SI_U_TOPIC = "S1-U";
    private static final String SIMEE_TOPIC = "DETAIL-CDR-s1mme";
    private static final String S6A_TOPIC = "S6a-topic";

    private static Map<String, Long> totalCountMap = new ConcurrentHashMap<>();
    private static Map<String, Long> currentCountMap = new ConcurrentHashMap<>();
    private static Map<String, Long> startTimeMap = new ConcurrentHashMap<>();

    static {
        totalCountMap.put(ZTE_A_TOPIC, 0L);
        totalCountMap.put(SI_U_TOPIC, 0L);
        totalCountMap.put(SIMEE_TOPIC, 0L);
        totalCountMap.put(S6A_TOPIC, 0L);

        currentCountMap.put(ZTE_A_TOPIC, 0L);
        currentCountMap.put(SI_U_TOPIC, 0L);
        currentCountMap.put(SIMEE_TOPIC, 0L);
        currentCountMap.put(S6A_TOPIC, 0L);

        startTimeMap.put(ZTE_A_TOPIC, System.currentTimeMillis());
        startTimeMap.put(SI_U_TOPIC, System.currentTimeMillis());
        startTimeMap.put(SIMEE_TOPIC, System.currentTimeMillis());
        startTimeMap.put(S6A_TOPIC, System.currentTimeMillis());
    }

    /**
     * 日志提示
     */
    private void logTip(String topicName, String message) {
        long countTotal = totalCountMap.get(topicName);
        totalCountMap.put(topicName, ++countTotal);
        long countCurrent = currentCountMap.get(topicName);
        currentCountMap.put(topicName, ++countCurrent);
        LOGGER.debug("receive topic='{}' message='{}'", topicName, message);
        long timeMilliscond = System.currentTimeMillis() - startTimeMap.get(topicName);
        if (countTotal % 500000 == 0 || timeMilliscond > 60 * 1000L) {
            LOGGER.info("Topic '{}' : {}",
                    topicName, new logTip(countTotal, countCurrent, (double) timeMilliscond / 1000, (int) (countCurrent * 1000 / timeMilliscond)));
            startTimeMap.put(topicName, System.currentTimeMillis());
            currentCountMap.put(topicName, 0L);
        }
    }

    @KafkaListener(topics = ZTE_A_TOPIC)
    public void processZteaMessage(String message) {
        logTip(ZTE_A_TOPIC, message);
    }


    @KafkaListener(topics = SI_U_TOPIC)
    public void processSiuMessage(String message) {
        logTip(SI_U_TOPIC, message);
    }


    @KafkaListener(topics = SIMEE_TOPIC)
    public void processS1mmeMessage(String message) {
        logTip(SIMEE_TOPIC, message);
    }

    @KafkaListener(topics = S6A_TOPIC)
    public void processS6aMessage(String message) {
        logTip(S6A_TOPIC, message);
    }
}

class logTip {
    private long total;
    private long currentCount;
    private Double timeUse;
    private int rate;

    public logTip() {
    }

    public logTip(long total, long currentCount, Double timeUse, int rate) {
        this.total = total;
        this.currentCount = currentCount;
        this.timeUse = timeUse;
        this.rate = rate;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(long currentCount) {
        this.currentCount = currentCount;
    }

    public Double getTimeUse() {
        return timeUse;
    }

    public void setTimeUse(Double timeUse) {
        this.timeUse = timeUse;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    @Override
    public String toString() {
        return "logTip{" +
                "total=" + total +
                ", count=" + currentCount +
                ", timeUse=" + timeUse + "s" +
                ", rate=" + rate + "/s" +
                '}';
    }
}
