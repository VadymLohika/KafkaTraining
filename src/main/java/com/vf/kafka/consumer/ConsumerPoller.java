package com.vf.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;

@Component
public class ConsumerPoller {

//    @Autowired
//    private KafkaConsumer<String, String> consumer;

    @Autowired
    private ConsumerProvider consumerProvider;

    @Autowired
    private ExecutorService executorService;

    private Logger logger = LoggerFactory.getLogger(ConsumerPoller.class);

    private int consumerCounter = 0;

    public void addNewConsumer(String topic){
        KafkaConsumer<String, String> consumer = consumerProvider.getConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        executorService.submit(getPollerTask(consumer, ++consumerCounter));
    }


    private Runnable getPollerTask(KafkaConsumer<String, String> consumer, int consumerCounter) {

        return () -> {
            try {

                logger.info("Starting consumer " + consumerCounter);
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                    for (ConsumerRecord<String, String> record : records) {
                        StringBuilder stringBuffer = new StringBuilder();
                        stringBuffer.append("Received message, ");
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
//                    if (custCountryMap.countainsKey(record.value())) {
//                        updatedCount = custCountryMap.get(record.value()) + 1;
//                    }
//                    custCountryMap.put(record.value(), updatedCount)
//
//                    JSONObject json = new JSONObject(custCountryMap);
//                    System.out.println(json.toString(4)) 4
                    }
                }
            } finally {
                consumer.close();
            }
        };
    }

//    private String getTopicsName(KafkaConsumer<String, String> consumer){
//
//        Map<String, List<PartitionInfo>> map = consumer.listTopics();
//        Map<String, String> m = new HashMap<>();
//        for(Map.Entry<String, List<PartitionInfo>> en : consumer.listTopics().entrySet()){
//
//        }
//
//
//    }

}
