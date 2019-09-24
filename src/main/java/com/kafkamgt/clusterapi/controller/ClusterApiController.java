package com.kafkamgt.clusterapi.controller;

import com.kafkamgt.clusterapi.services.ManageKafkaComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Set;

@RestController
@RequestMapping("/topics")
public class ClusterApiController {

    private static Logger LOG = LoggerFactory.getLogger(ClusterApiController.class);

    @Autowired
    ManageKafkaComponents kafkaTopics;

    @RequestMapping(value = "/getTopics/{env}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Set<String>> getTopics(@PathVariable String env){
        Set<String> topics = kafkaTopics.loadTopics(env);

        return new ResponseEntity<>(topics, HttpStatus.OK);
    }

    @RequestMapping(value = "/getAcls/{env}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Set<HashMap<String,String>>> getAcls(@PathVariable String env){
        Set<HashMap<String,String>> acls = kafkaTopics.loadAcls(env);

        return new ResponseEntity<>(acls, HttpStatus.OK);
    }

    @PostMapping(value = "/createTopics")
    public ResponseEntity<String> createTopics(@RequestBody MultiValueMap<String, String> topicRequest){
        kafkaTopics.createTopic(
                topicRequest.get("topicName").get(0),
                topicRequest.get("partitions").get(0),
                topicRequest.get("rf").get(0),
                topicRequest.get("env").get(0)
            );

        return new ResponseEntity<String>("success", HttpStatus.OK);
    }

    @PostMapping(value = "/createAcls")
    public ResponseEntity<String> createAcls(@RequestBody MultiValueMap<String, String> topicRequest){

//        if(!utils.validateLicense()){
//            LOG.info("Invalid License !!");
//            return new ResponseEntity<String>("", HttpStatus.FORBIDDEN);
//        }
        LOG.info("----"+topicRequest.get("topicName"));
        String aclType = topicRequest.get("aclType").get(0);
        if(aclType.equals("Producer"))
            kafkaTopics.createProducerAcl(topicRequest.get("topicName").get(0),topicRequest.get("env").get(0),
                    topicRequest.get("acl_ip").get(0),topicRequest.get("acl_ssl").get(0));
        else
            kafkaTopics.createConsumerAcl(topicRequest.get("topicName").get(0),topicRequest.get("env").get(0),
                    topicRequest.get("acl_ip").get(0),topicRequest.get("acl_ssl").get(0), topicRequest.get("consumerGroup").get(0));

        return new ResponseEntity<String>("success", HttpStatus.OK);
    }


    @PostMapping(value = "/postSchema")
    public ResponseEntity<String> postSchema(@RequestBody MultiValueMap<String, String> fullSchemaDetails){
        String topicName= fullSchemaDetails.get("topicName").get(0);
        String schemaFull = fullSchemaDetails.get("fullSchema").get(0);
        String env = fullSchemaDetails.get("env").get(0);

        String result = kafkaTopics.postSchema(topicName,schemaFull,env);

        return new ResponseEntity<String>("Status:"+result, HttpStatus.OK);
    }


}
