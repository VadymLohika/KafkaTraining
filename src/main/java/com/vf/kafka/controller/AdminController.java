package com.vf.kafka.controller;

import com.sun.tools.javac.util.ListBuffer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Set;

@RestController
@RequestMapping(path = "/admin")
public class AdminController {

    @Autowired
    private KafkaAdmin admin;

    @RequestMapping(path = "/topic/add/{topicName}/{partitions}")
    public Object addTopic(@PathVariable("topicName") String topicName,
                           @PathVariable("partitions") int partitions) {

        try (AdminClient client = AdminClient.create(admin.getConfig())) {
            NewTopic newTopic = TopicBuilder.name(topicName)
                    .partitions(partitions)
                    .replicas(1)
                    .compact()
                    .build();

            client.createTopics(Collections.singletonList(newTopic));
        }
        return "Created topic " + topicName;
    }

    @RequestMapping(path = "/topic/delete/{topicName}")
    public Object removeTopic(@PathVariable("topicName") String topicName) {

        try (AdminClient client = AdminClient.create(admin.getConfig())) {
            client.deleteTopics(Collections.singletonList(topicName));
        }
        return "Deleted topic " + topicName;
    }

    @RequestMapping(path = "/topic/getAll")
    public Object listTopics(){
        ListBuffer<String> topicNames = new ListBuffer<>();
        try (AdminClient client = AdminClient.create(admin.getConfig())) {
            ListTopicsResult listTopicsResult = client.listTopics();
            KafkaFuture<Set<String>> names = listTopicsResult.names();
            topicNames.addAll(names.get());
        } catch (Exception e) {
            return e.toString();
        }

        return topicNames.toString();
    }
}
