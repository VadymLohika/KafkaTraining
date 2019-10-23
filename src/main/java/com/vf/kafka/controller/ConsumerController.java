package com.vf.kafka.controller;

import com.vf.kafka.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/consumer")
public class ConsumerController {

    @Autowired
    private ConsumerService consumerPoller;

    @RequestMapping(path = "/add/topicName/{topicName}")
    public Object addConsumer(@PathVariable("topicName") String topicName){
        int consumerId = consumerPoller.addNewConsumer(topicName);
        return " Started consumer with id " + consumerId;
    }

    @RequestMapping(path = "/delete/topicName/{topicName}/id/{id}")
    public Object deleteConsumer(@PathVariable("topicName") String topicName,
                                 @PathVariable("id") int id){
        consumerPoller.deleteConsumer(topicName, id);
        return " Stopped consumer with id " + id;
    }

}
