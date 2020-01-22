package com.kafkamgt.clusterapi.controller;

import com.kafkamgt.clusterapi.services.ManageKafkaComponents;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/topics")
@Slf4j
public class ClusterApiController {

    @Autowired
    ManageKafkaComponents manageKafkaComponents;

    @RequestMapping(value = "/getApiStatus", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> getApiStatus(){
        return new ResponseEntity<>("ONLINE", HttpStatus.OK);
    }

    @RequestMapping(value = "/getStatus/{env}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> getStatus(@PathVariable String env){
        String envStatus = manageKafkaComponents.getStatus(env);

        return new ResponseEntity<>(envStatus, HttpStatus.OK);
    }

    @RequestMapping(value = "/getTopics/{env}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Set<String>> getTopics(@PathVariable String env){
        Set<String> topics = manageKafkaComponents.loadTopics(env);
        return new ResponseEntity<>(topics, HttpStatus.OK);
    }

    @RequestMapping(value = "/getAcls/{env}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<Set<HashMap<String,String>>> getAcls(@PathVariable String env){
        Set<HashMap<String,String>> acls = manageKafkaComponents.loadAcls(env);

        return new ResponseEntity<>(acls, HttpStatus.OK);
    }

    @PostMapping(value = "/createTopics")
    public ResponseEntity<String> createTopics(@RequestBody MultiValueMap<String, String> topicRequest){
        try {
            manageKafkaComponents.createTopic(
                    topicRequest.get("topicName").get(0),
                    topicRequest.get("partitions").get(0),
                    topicRequest.get("rf").get(0),
                    topicRequest.get("env").get(0)
                );
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<String>("failure "+e, HttpStatus.OK);
        }

        return new ResponseEntity<String>("success", HttpStatus.OK);
    }

    @PostMapping(value = "/createAcls")
    public ResponseEntity<String> createAcls(@RequestBody MultiValueMap<String, String> topicRequest){

        try {
            String aclType = topicRequest.get("aclType").get(0);

            if (aclType.equals("Producer"))
                manageKafkaComponents.createProducerAcl(topicRequest.get("topicName").get(0),
                        topicRequest.get("env").get(0),
                        topicRequest.get("acl_ip").get(0), topicRequest.get("acl_ssl").get(0));
            else
                manageKafkaComponents.createConsumerAcl(topicRequest.get("topicName").get(0),
                        topicRequest.get("env").get(0),
                        topicRequest.get("acl_ip").get(0), topicRequest.get("acl_ssl").get(0), topicRequest.get("consumerGroup").get(0));

            return new ResponseEntity<String>("success", HttpStatus.OK);
        }catch(Exception e){
            return new ResponseEntity<String>("failure "+e.getMessage(), HttpStatus.OK);
        }
    }

    @PostMapping(value = "/postSchema")
    public ResponseEntity<String> postSchema(@RequestBody MultiValueMap<String, String> fullSchemaDetails){
        try {
            String topicName = fullSchemaDetails.get("topicName").get(0);
            String schemaFull = fullSchemaDetails.get("fullSchema").get(0);
            String env = fullSchemaDetails.get("env").get(0);

            String result = manageKafkaComponents.postSchema(topicName, schemaFull, env);
            return new ResponseEntity<>("Status:"+result, HttpStatus.OK);
        }catch(Exception e){
            return new ResponseEntity<>("failure "+e.getMessage(), HttpStatus.OK);
        }
    }


}
