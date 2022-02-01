package com.kafkamgt.clusterapi.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@Slf4j
public class KafkaConnectService {

    public RestTemplate getRestTemplate(){
        return new RestTemplate();
    }

    public HashMap<String, String> deleteConnector(String environmentVal, String connectorName) {
        log.info("Into deleteConnector {} {}", environmentVal, connectorName);
        HashMap<String, String> result = new HashMap<>();

        if (environmentVal == null)
            return null;
        String connectorsUrl = "http://" + environmentVal + "/connectors/" + connectorName;
        RestTemplate restTemplate = getRestTemplate();

        try {
            restTemplate.delete(connectorsUrl, String.class);
        } catch (RestClientException e) {
            log.error("Error in deleting connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        result.put("result","success");
        return result;
    }

    public HashMap<String, String> updateConnector(String environmentVal, String connectorName, String connectorConfig){
        log.info("Into updateConnector {} {} {}",  environmentVal, connectorName, connectorConfig);
        HashMap<String, String> result = new HashMap<>();

        if (environmentVal == null)
            return null;
        String connectorsUrl = "http://" + environmentVal + "/connectors/" + connectorName + "/config";
        RestTemplate restTemplate = getRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);

        try {
            restTemplate.put(connectorsUrl, request, String.class);
        } catch (RestClientException e) {
            log.error("Error in updating connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        result.put("result","success");

        return result;
    }

    public HashMap<String, String> postNewConnector(String environmentVal, String connectorConfig){
        log.info("Into postNewConnector {} {}",  environmentVal, connectorConfig);
        HashMap<String, String> result = new HashMap<>();
//        String connectorConfig = "{\n" +
//                "    \"name\": \"local-file-source11\",\n" +
//                "\t\"config\": {\n" +
//                "        \"connector.class\": \"FileStreamSource\",\n" +
//                "        \"tasks.max\": \"1\",\n" +
//                "        \"topic\": \"test-topic\",\n" +
//                "\t\t\"file\":\"C:/Software/confluent-5.3.1-2.12/dev/etc/kafka/test1.txt\"\n" +
//                "    }\n" +
//                "}";
//
//        String environmentVal = "localhost:8083";
        if (environmentVal == null)
            return null;
        String connectorsUrl = "http://" + environmentVal + "/connectors";
        RestTemplate restTemplate = getRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<String> request = new HttpEntity<>(connectorConfig, headers);
        ResponseEntity<String> responseNew ;
        try {
            responseNew = restTemplate.postForEntity(connectorsUrl, request, String.class);
        } catch (RestClientException e) {
            log.error("Error in registering new connector " + e.toString());
            result.put("result", "error");
            result.put("errorText", e.toString().replaceAll("\"",""));
            return result;
        }
        if(responseNew.getStatusCodeValue() == 201) {
            result.put("result","success");
        }
        else {
            result.put("result","failure");
        }
        return result;
    }

    public ArrayList<String> getConnectors(String environmentVal){
        try {
            log.info("Into getConnectors {}",  environmentVal);
            if (environmentVal == null)
                return null;
            String connectorsUrl = "http://" + environmentVal;

            String uri = connectorsUrl + "/connectors";
            RestTemplate restTemplate = getRestTemplate();
            Map<String, String> params = new HashMap<>();
            ResponseEntity<ArrayList> responseList = restTemplate.getForEntity(uri, ArrayList.class, params);

            log.info("connectors list " + responseList);
            return  responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting connectors " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public LinkedHashMap<String, Object> getConnectorDetails(String connector, String environmentVal){
        try {
            log.info("Into getConnectorDetails {}",  environmentVal);
            if (environmentVal == null)
                return null;

            String connectorsUrl = "http://" + environmentVal + "/connectors" + "/" + connector;

            RestTemplate restTemplate = getRestTemplate();
            Map<String, String> params = new HashMap<>();

            ResponseEntity<LinkedHashMap> responseList = restTemplate.getForEntity(connectorsUrl, LinkedHashMap.class, params);
            log.info("connectors list " + responseList);

            return responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting connector detail " + e.getMessage());
            return new LinkedHashMap<>();
        }
    }
}
