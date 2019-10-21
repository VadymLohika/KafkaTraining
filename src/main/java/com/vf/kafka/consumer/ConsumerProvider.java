package com.vf.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

@Component
public class ConsumerProvider {

    @Autowired
    private KafkaConsumer<String, String> consumer;

    @Lookup
    public KafkaConsumer<String, String> getConsumer(){
        return consumer;
    }
}
