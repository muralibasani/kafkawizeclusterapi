package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
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

    private static final String REQUEST_TIMEOUT_CONFIG = "10000";
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

    public Properties getSslProperties(String environment){
        Properties props = getSslConfig();

        props.put("bootstrap.servers", environment);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SSL");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"kafakwizeclientssl");
        setOtherConfig(props);

        return props;
    }

    public Properties getSaslPlainProperties(String environment){
        Properties props = new Properties();

        props.put("bootstrap.servers", environment);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"kafakwizeclientsaslplain");
        setOtherConfig(props);
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.plain"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG,env.getProperty("kafkasasl.jaasconfig.plain"));

        return props;
    }

    public Properties getSaslSsl_PlainMechanismProperties(String environment){
        Properties props = getSslConfig();

        props.put("bootstrap.servers", environment);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"kafakwizeclientsaslssl");
        setOtherConfig(props);
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.plain"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG,env.getProperty("kafkasasl.jaasconfig.plain"));

        return props;
    }

    public Properties getSaslSsl_GSSAPIMechanismProperties(String environment){
        Properties props = getSslConfig();

        props.put("bootstrap.servers", environment);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG,"kafakwizeclientsaslssl");
        setOtherConfig(props);
        props.put(SaslConfigs.SASL_MECHANISM, env.getProperty("kafkasasl.saslmechanism.gssapi"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG,env.getProperty("kafkasasl.jaasconfig.gssapi"));

        return props;
    }

    private Properties getSslConfig(){
        Properties props = new Properties();

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,env.getProperty("kafkassl.truststore.location"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,env.getProperty("kafkassl.truststore.pwd"));
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,env.getProperty("kafkassl.keystore.location"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,env.getProperty("kafkassl.keystore.pwd"));
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,env.getProperty("kafkassl.key.pwd"));
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,"JKS");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,"JKS");
        props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2,TLSv1.1");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        return props;
    }

    private void setOtherConfig(Properties props) {
        props.put(AdminClientConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, env.getProperty("kafkawize.request.timeout.ms"));
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_CONFIG );
    }
}
