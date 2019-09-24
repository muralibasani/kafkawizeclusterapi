package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class GetAdminClient {

    @Autowired
    Environment env;

    public AdminClient getAdminClient(String envHost){

        String envOnlyHost = envHost.substring(0,envHost.indexOf(":"));
        String ssl_acl_enabled = env.getProperty(envOnlyHost+".ssl_acl.enabled");
        if(ssl_acl_enabled==null)
            return AdminClient.create(getPlainProperties(envHost));
        else if(ssl_acl_enabled!=null && ssl_acl_enabled.equals("true"))
            return AdminClient.create(getSslProperties(envHost));
        else
            return AdminClient.create(getPlainProperties(envHost));
    }

    public Properties getPlainProperties(String environment){
        Properties props = new Properties();

        props.put("bootstrap.servers",environment);

        return props;
    }

    public Properties getSslProperties(String environment){
        Properties props = new Properties();

        String envOnlyHost = environment.substring(0,environment.indexOf(":"));
        String bootStrapServer = envOnlyHost +":" +env.getProperty("kafkassl."+envOnlyHost+".port");
        props.put("bootstrap.servers",bootStrapServer);
        props.put("ssl.truststore.location",env.getProperty("kafkassl."+envOnlyHost+".truststore.location"));
        props.put("ssl.truststore.password",env.getProperty("kafkassl."+envOnlyHost+".truststore.pwd"));
        props.put("ssl.keystore.location",env.getProperty("kafkassl."+envOnlyHost+".keystore.location"));
        props.put("ssl.keystore.password",env.getProperty("kafkassl."+envOnlyHost+".keystore.pwd"));
        props.put("ssl.key.password",env.getProperty("kafkassl."+envOnlyHost+".key.pwd"));
        props.put("ssl.keystore.type","JKS");
        props.put("ssl.truststore.type","JKS");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SSL");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"testclient");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,"1000000");

        return props;
    }
}
