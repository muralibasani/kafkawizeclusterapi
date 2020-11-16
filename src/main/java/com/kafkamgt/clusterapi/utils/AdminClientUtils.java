package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Properties;

@Service
public class AdminClientUtils {

    @Autowired
    Environment env;

    private static final String RETRIES_CONFIG = "5";
    private static final String RETRY_BACKOFF_MS_CONFIG = "5000";

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

        props.put("bootstrap.servers",environment);
        setOtherConfig(props);

        return props;
    }

    private void setOtherConfig(Properties props) {
        props.put(AdminClientConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, env.getProperty("kafkawize.request.timeout.ms"));
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_CONFIG );
    }
}
