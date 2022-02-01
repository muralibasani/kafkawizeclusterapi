package com.kafkamgt.clusterapi.controller;

import com.kafkamgt.clusterapi.services.KafkaConnectService;
import com.kafkamgt.clusterapi.services.ManageKafkaComponents;
import com.kafkamgt.clusterapi.services.MonitoringService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/topics")
@Slf4j
public class KafkaConnectController {

    @Autowired
    KafkaConnectService kafkaConnectService;

    @RequestMapping(value = "/getAllConnectors/{kafkaConnectHost}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<ArrayList<String>> getAllConnectors(@PathVariable String kafkaConnectHost){
        return new ResponseEntity<>(kafkaConnectService.getConnectors(kafkaConnectHost), HttpStatus.OK);
    }

    @RequestMapping(value = "/getConnectorDetails/{connectorName}/{kafkaConnectHost}", method = RequestMethod.GET,produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<LinkedHashMap<String, Object>> getConnectorDetails(@PathVariable String connectorName,
                                                                 @PathVariable String kafkaConnectHost){
        return new ResponseEntity<>(kafkaConnectService.getConnectorDetails(connectorName, kafkaConnectHost), HttpStatus.OK);
    }

    @PostMapping(value = "/postConnector")
    public ResponseEntity<HashMap<String, String>> postConnector(@RequestBody MultiValueMap<String, String> fullConnectorConfig){
        String env = fullConnectorConfig.get("env").get(0);
        String connectorConfig = fullConnectorConfig.get("connectorConfig").get(0);

        HashMap<String, String> result = kafkaConnectService.postNewConnector(env, connectorConfig);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "/updateConnector")
    public ResponseEntity<HashMap<String, String>> updateConnector(@RequestBody MultiValueMap<String, String> fullConnectorConfig){
        String env = fullConnectorConfig.get("env").get(0);
        String connectorName = fullConnectorConfig.get("connectorName").get(0);
        String connectorConfig = fullConnectorConfig.get("connectorConfig").get(0);

        HashMap<String, String> result = kafkaConnectService.updateConnector(env, connectorName, connectorConfig);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "/deleteConnector")
    public ResponseEntity<HashMap<String, String>> deleteConnector(@RequestBody MultiValueMap<String, String> connectorConfig){
        String env = connectorConfig.get("env").get(0);
        String connectorName = connectorConfig.get("connectorName").get(0);

        HashMap<String, String> result = kafkaConnectService.deleteConnector(env, connectorName);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}
