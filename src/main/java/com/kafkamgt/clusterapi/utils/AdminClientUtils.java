package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Properties;

@Service
public class AdminClientUtils {

    @Autowired
    Environment env;

    @Value("${kafkawize.request.timeout.ms:15000}")
    private
    String requestTimeOutMs;

    @Value("${kafkawize.retries.config:25}")
    private
    String retriesConfig;

    @Value("${kafkawize.retry.backoff.ms:15000}")
    private
    String retryBackOffMsConfig;

    private final HashMap<String, AdminClient> adminClientsMap = new HashMap<>();;

    public RestTemplate getRestTemplate(){
        return new RestTemplate();
    }

    public AdminClient getAdminClient(String envHost, String protocol){

        AdminClient adminClient;

        if(!adminClientsMap.containsKey(envHost))
            adminClient = AdminClient.create(getPlainProperties(envHost));
        else {
            adminClient = adminClientsMap.get(envHost);
        }

        try{
            if(adminClient == null)
                return null;

            adminClient.listTopics().names().get();
            if(!adminClientsMap.containsKey(envHost))
                adminClientsMap.put(envHost, adminClient);
            return adminClient;
        }catch (Exception e){
            adminClientsMap.remove(envHost);
            adminClient.close();
            return null;
        }
    }

    public Properties getPlainProperties(String environment){
        Properties props = new Properties();

        props.put("bootstrap.servers", environment);
        setOtherConfig(props);

        return props;
    }

    private void setOtherConfig(Properties props) {
        props.put(AdminClientConfig.RETRIES_CONFIG, retriesConfig);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackOffMsConfig);
    }
}
