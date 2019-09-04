package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class LoadKafkaProperties {
    public Properties getPlainProperties(String environment){
        Properties props = new Properties();

        props.put("bootstrap.servers",environment);

        return props;
    }

    public Properties getSslProperties(String environment){
        Properties props = new Properties();

        props.put("bootstrap.servers","localhost:9093");
        props.put("ssl.truststore.location","C:/Software/certs_kafka_client/kafka.truststore.jks");
        props.put("ssl.truststore.password","pwd");
        props.put("ssl.keystore.location","C:/Software/certs_kafka_client/kafka.keystore.jks");
        props.put("ssl.keystore.password","pwd");
        props.put("ssl.keystore.type","JKS");
        props.put("ssl.truststore.type","JKS");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SSL");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"testclient");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,"1000000");

        return props;
    }
}
