package com.vf.kafka.controller;

import com.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileReader;
import java.util.List;

@RestController
@RequestMapping(path = "/producer")
public class ProducerController {

    private Logger logger = LoggerFactory.getLogger(ProducerController.class);

    private volatile List<String[]> entries;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ResourceLoader resourceLoader;

    @RequestMapping(path = "/sendMessage/{topicName}")
    public void produceMessages(@PathVariable("topicName") String topicName) {
        getMessagesFromCSV().forEach(message -> kafkaTemplate.send(topicName, message[0], message[1]));
    }

    private List<String[]> getMessagesFromCSV(){
        //Some kind of primitive caching.
        if(entries != null) {
            return entries;
        }
        try {
            File file = new File(
                    getClass().getClassLoader().getResource("messages_source.csv").getFile()
            );
            CSVReader reader = new CSVReader(new FileReader(file));
            entries = reader.readAll();
        } catch (Exception e){
            logger.error("Error in parsing CSV file " + e.getMessage());
            throw new RuntimeException(e);
        }

        return entries;

    }
}
