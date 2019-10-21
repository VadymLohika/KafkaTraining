package com.vf.kafka.controller;

import com.vf.kafka.consumer.ConsumerPoller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/consumer")
public class ConsumerController {

    @Autowired
    private ConsumerPoller consumerPoller;

    @RequestMapping(path = "/add/{topicName}")
    public void addConsumer(@PathVariable("topicName") String topicName){
        consumerPoller.addNewConsumer(topicName);
    }

}
