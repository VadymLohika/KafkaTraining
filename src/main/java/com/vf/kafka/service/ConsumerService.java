package com.vf.kafka.service;

import com.vf.kafka.consumer.ConsumerProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;

@Component
public class ConsumerService {

//    @Autowired
//    private KafkaConsumer<String, String> consumer;

    @Autowired
    private ConsumerProvider consumerProvider;

    @Autowired
    private ExecutorService executorService;

    private Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private int consumerCounter = 0;

    private Map<String, Map<Integer, KafkaConsumer>> startedConsumers = new HashMap<>();

    public int addNewConsumer(String topic){
        KafkaConsumer<String, String> consumer = consumerProvider.getConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        executorService.submit(getPollerTask(consumer, ++consumerCounter));
        if(startedConsumers.containsKey(topic)) {
            Map<Integer, KafkaConsumer> kafkaConsumerMap = startedConsumers.get(topic);
            kafkaConsumerMap.put(consumerCounter, consumer);
        }
        else {
            Map<Integer, KafkaConsumer> kafkaConsumerMap = new HashMap<>();
            kafkaConsumerMap.put(consumerCounter, consumer);
            startedConsumers.put(topic, kafkaConsumerMap);
        }
        return consumerCounter;
    }

    public void deleteConsumer(String topic, int id){
        if (startedConsumers.containsKey(topic) && startedConsumers.get(topic).containsKey(id)) {
            KafkaConsumer consumer = startedConsumers.get(topic).get(id);
            consumer.wakeup();
        }
    }


    private Runnable getPollerTask(KafkaConsumer<String, String> consumer, int consumerCounter) {

        return () -> {
            try {

                logger.info("Starting consumer " + consumerCounter);
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                    for (ConsumerRecord<String, String> record : records) {
                        StringBuilder stringBuffer = new StringBuilder();
                        stringBuffer.append("Consumer -  ");
                        stringBuffer.append(consumerCounter);
                        stringBuffer.append(" received message, ");
                        stringBuffer.append("topic = ");
                        stringBuffer.append(record.topic());
                        stringBuffer.append(" partition = ");
                        stringBuffer.append(record.partition());
                        stringBuffer.append(" offset = ");
                        stringBuffer.append(record.offset());
                        stringBuffer.append(" key = ");
                        stringBuffer.append(record.key());
                        stringBuffer.append(" value = ");
                        stringBuffer.append(record.value());


                        logger.info(stringBuffer.toString());
                    }
                }
            } finally {
                consumer.close();
            }
        };
    }

}
