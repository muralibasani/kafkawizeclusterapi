package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.models.AclIPPrincipleType;
import com.kafkamgt.clusterapi.utils.AdminClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class ManageKafkaComponents {

    @Autowired
    Environment env;

    @Autowired
    AdminClientUtils getAdminClient;

    private static final long timeOutSecsForAcls = 5;

    private static final long timeOutSecsForTopics = 5;

    @Value("${kafkawize.schemaregistry.compatibility.default:BACKWARD}")
    private
    String defaultSchemaCompatibility;

    public ManageKafkaComponents(){}

    public ManageKafkaComponents(Environment env, AdminClientUtils getAdminClient){
        this.env = env;
        this.getAdminClient = getAdminClient;
    }

//    public String reloadTruststore(String protocol, String clusterName){
//        getAdminClient.removeSSLElementFromAdminClientMap(protocol, clusterName);
//        return "success";
//    }

    public String getStatus(String environment, String protocol, String clusterName, String clusterType){
        //log.info("getStatus {} {}", environment, protocol);
        if(clusterType.equals("kafka"))
            return getStatusKafka(environment, protocol, clusterName);
        else if(clusterType.equals("schemaregistry"))
            return getSchemaRegistryStatus(environment, protocol);
        else if(clusterType.equals("kafkaconnect"))
            return getKafkaConnectStatus(environment, protocol);
        else
            return "OFFLINE";
    }

    private String getKafkaConnectStatus(String environment, String protocol) {
        String connectUrl = "http://" + environment + "/connectors";

        String uri = connectUrl ;
        RestTemplate restTemplate = getAdminClient.getRestTemplate();
        Map<String, String> params = new HashMap<String, String>();

        try {
            ResponseEntity<Object> responseNew = restTemplate.getForEntity(uri, Object.class, params);
            return "ONLINE";
        } catch (RestClientException e) {
            e.printStackTrace();
            return "OFFLINE";
        }
    }

    private String getStatusKafka(String environment, String protocol, String clusterName) {
        try {
            AdminClient client = getAdminClient.getAdminClient(environment, protocol, clusterName);
            if(client != null) {
                return "ONLINE";
            }
            else
                return "OFFLINE";

        } catch (Exception e){
            e.printStackTrace();
            return "OFFLINE";
        }
    }

    public String getSchemaRegistryStatus(String environment, String protocol){
        String schemaRegistryUrl = "http://" + environment + "/subjects";

        String uri = schemaRegistryUrl ;
        RestTemplate restTemplate = getAdminClient.getRestTemplate();
        Map<String, String> params = new HashMap<String, String>();

        try {
            ResponseEntity<Object> responseNew = restTemplate.getForEntity(uri, Object.class, params);
            return "ONLINE";
        } catch (RestClientException e) {
            e.printStackTrace();
            return "OFFLINE";
        }
    }

    public synchronized Set<HashMap<String,String>> loadAcls(String environment, String protocol, String clusterName) throws Exception {
        log.info("loadAcls {} {}", environment, protocol);
        Set<HashMap<String,String>> acls = new HashSet<>();

        AdminClient client = getAdminClient.getAdminClient(environment, protocol, clusterName);
        if(client == null)
            throw new Exception("Cannot connect to cluster.");

         try {
             AclBindingFilter aclBindingFilter = AclBindingFilter.ANY;
             DescribeAclsResult aclsResult = client.describeAcls(aclBindingFilter);

                aclsResult.values().get(timeOutSecsForAcls, TimeUnit.SECONDS)
                    .forEach(aclBinding -> {
                        if (aclBinding.pattern().patternType().name().equals("LITERAL")) {
                            HashMap<String, String> aclbindingMap = new HashMap<>();
                            aclbindingMap.put("host", aclBinding.entry().host());
                            aclbindingMap.put("principle", aclBinding.entry().principal());
                            aclbindingMap.put("operation", aclBinding.entry().operation().toString());
                            aclbindingMap.put("permissionType", aclBinding.entry().permissionType().toString());
                            aclbindingMap.put("resourceType", aclBinding.pattern().resourceType().toString());
                            aclbindingMap.put("resourceName", aclBinding.pattern().name());

                            if (!aclBinding.pattern().resourceType().toString().equals("CLUSTER")) {
                                if (aclBinding.entry().operation().toString().equals("WRITE") ||
                                        aclBinding.entry().operation().toString().equals("READ"))
                                    acls.add(aclbindingMap);
                            }
                        }
            });
        }catch (Exception e){
             log.error("Error "+e.getMessage());
         }

        return acls;
    }

    public synchronized Set<HashMap<String,String>> loadTopics(String environment, String protocol,
                                                               String clusterName) throws Exception {
        log.info("loadTopics {} {}", environment, protocol);
        AdminClient client = getAdminClient.getAdminClient(environment, protocol, clusterName);
        Set<HashMap<String,String>> topics = new HashSet<>();
        if(client == null)
            throw new Exception("Cannot connect to cluster.");

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions = listTopicsOptions.listInternal(false);

        ListTopicsResult topicsResult = client.listTopics(listTopicsOptions);

        try {
            DescribeTopicsResult s = client.describeTopics(new ArrayList<>(topicsResult.names().get()));
            Map<String, TopicDescription> topicDesc  = s.all().get(timeOutSecsForTopics,TimeUnit.SECONDS);
            Set<String> keySet = topicDesc.keySet();
            keySet.remove("_schemas");
            List<String> lstK = new ArrayList<>(keySet);
            HashMap<String,String> hashMap;
            for (String topicName : lstK) {
                hashMap = new HashMap<>();
                hashMap.put("topicName",topicName);
                hashMap.put("replicationFactor","" + topicDesc.get(topicName).partitions().get(0).replicas().size());
                hashMap.put("partitions", "" + topicDesc.get(topicName).partitions().size());
                if(!topicName.startsWith("_confluent"))
                    topics.add(hashMap);
            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e.getMessage());
        }
        return topics;
    }

    public synchronized String createTopic(String name, String partitions, String replicationFactor,
                              String environment, String protocol, String clusterName) throws Exception {

        log.info("createTopic Name: {} Partitions:{} Replication factor:{} Environment:{} Protocol:{} clusterName:{}",
                name, partitions, replicationFactor, environment, protocol, clusterName);

        AdminClient client ;
        try{
            client = getAdminClient.getAdminClient(environment, protocol, clusterName);
            if(client == null)
                throw new Exception("Cannot connect to cluster.");

            NewTopic topic = new NewTopic(name, Integer.parseInt(partitions),
                    Short.parseShort(replicationFactor));

            CreateTopicsResult result = client.createTopics(Collections.singletonList(topic));
            result.values().get(name).get(timeOutSecsForTopics,TimeUnit.SECONDS);
        } catch (KafkaException e) {
            String errorMessage = "Invalid properties: ";
            log.error(errorMessage, e);
            throw e;
        } catch (NumberFormatException e) {
            String errorMessage = "Invalid replica assignment string";
            log.error(errorMessage, e);
            throw e;
        } catch (ExecutionException | InterruptedException e) {
            String errorMessage;
            if (e instanceof ExecutionException) {
                errorMessage = e.getCause().getMessage();
            } else {
                Thread.currentThread().interrupt();
                errorMessage = e.getMessage();
            }
            log.error("Unable to create topic {}, {}", name, errorMessage);
            throw e;
        }
        catch (Exception e){
            log.error(e.getMessage());
            e.printStackTrace();
            throw e;
        }

        return "success";
    }

    public synchronized String updateTopic(String topicName, String partitions, String replicationFactor,
                                           String environment, String protocol, String clusterName) throws Exception {

        log.info("updateTopic Name: {} Partitions:{} Replication factor:{} Environment:{} Protocol:{} clusterName:{}",
                topicName, partitions, replicationFactor, environment, protocol, clusterName);

        AdminClient client = getAdminClient.getAdminClient(environment, protocol, clusterName);

        if(client == null)
            throw new Exception("Cannot connect to cluster.");

        DescribeTopicsResult describeTopicsResult = client.describeTopics(Collections.singleton(topicName));
        TopicDescription result = describeTopicsResult.all().get(timeOutSecsForTopics,TimeUnit.SECONDS).get(topicName);

        if(result.partitions().size() > Integer.parseInt(partitions)){
            // delete topic and recreate
            deleteTopic(topicName, environment, protocol, clusterName);
            createTopic(topicName, partitions, replicationFactor, environment, protocol, clusterName);
        }
        else{
            // update config
//            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
//            ConfigEntry retentionEntry = new ConfigEntry(TopicConfig., "60000");
//            Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<>();
//            AlterConfigOp alterConfigOp = new AlterConfigOp();
//            client.incrementalAlterConfigs(updateConfig);

            Map<String, NewPartitions> newPartitionSet = new HashMap<>();
            newPartitionSet.put(topicName, NewPartitions.increaseTo(Integer.parseInt(partitions)));

            client.createPartitions(newPartitionSet);
        }

        return "success";
    }

    public synchronized void deleteTopic(String topicName, String environment, String protocol, String clusterName) throws Exception {

        log.info("deleteTopic Topic name:{} Env:{} Protocol:{} clusterName:{}", topicName, environment, protocol, clusterName);

        AdminClient client;
        try{
            client = getAdminClient.getAdminClient(environment, protocol, clusterName);
            if(client == null)
                throw new Exception("Cannot connect to cluster.");

            DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topicName));
            result.values().get(topicName).get(timeOutSecsForTopics, TimeUnit.SECONDS);
        } catch (KafkaException e) {
            String errorMessage = "Invalid properties: ";
            log.error(errorMessage, e);
            throw e;
        }  catch (ExecutionException | InterruptedException e) {
            String errorMessage;
            if (e instanceof ExecutionException) {
                errorMessage = e.getCause().getMessage();
            } else {
                Thread.currentThread().interrupt();
                errorMessage = e.getMessage();
            }
            log.error("Unable to delete topic {}, {}", topicName, errorMessage);

            throw e;
        }
        catch (Exception e){
            log.error(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }



    public synchronized String updateProducerAcl(String topicName, String environment, String protocol, String clusterName,
                                    String acl_ip, String acl_ssl, String aclOperation, String isPrefixAcl, String transactionalId,
                                                 String aclIpPrincipleType) {

        log.info("updateProducerAcl TopicName:{} Env:{} Protocol:{} AclIP:{} AclSSL:{} AclOperation:{} PrefixAcl:{} TxnId:{}" +
                        " aclIpPrincipleType: {}",
                topicName, environment, protocol, acl_ip, acl_ssl, aclOperation, isPrefixAcl, transactionalId, aclIpPrincipleType);
        AdminClient client;
        try {
            PatternType patternType;
            if(isPrefixAcl.equals("true"))
                patternType = PatternType.PREFIXED;
            else
                patternType = PatternType.LITERAL;

            client = getAdminClient.getAdminClient(environment, protocol, clusterName);
            if(client==null)
               return "failure";

            String host, principal;
            if(acl_ssl != null  && acl_ssl.trim().length()>0){
                acl_ssl = acl_ssl.trim();
//                if(acl_ssl.contains("CN") || acl_ssl.contains("cn"))
                if(aclIpPrincipleType.equals(AclIPPrincipleType.PRINCIPLE.name()))
                {
                    host = "*";
                    principal = "User:" + acl_ssl;

                    if(aclOperation.equals("Create")){
                        if(updateTopicProducerWriteAcls(topicName, client, patternType, host, principal))
                            return "Acl already exists. success";
                    }else{
                        List<AclBindingFilter> aclListArray = new ArrayList<>();

                        ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, topicName, patternType);
                        AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal, host, AclOperation.WRITE, AclPermissionType.ALLOW);
                        AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding1);

                        aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                        AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding2);

                        client.deleteAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                    }
                    updateTransactionalIdAclsForProducer(transactionalId, client, patternType, host, principal, aclOperation);
                }
            }

            if(acl_ip != null && acl_ip.trim().length()>0){
                acl_ip = acl_ip.trim();

                host = acl_ip;
                principal = "User:*";

                if(aclOperation.equals("Create")){
                    if(updateTopicProducerWriteAcls(topicName, client, patternType, host, principal))
                        return "Acl already exists. success";
                }else{
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, topicName, patternType);
                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal, host,
                            AclOperation.WRITE, AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding2);

                    client.deleteAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                }
                // Update transactional id acls
                updateTransactionalIdAclsForProducer(transactionalId, client, patternType, host, principal, aclOperation);
            }

        }catch (Exception e){
            e.printStackTrace();
            return "failure";
        }

        return "success";
        }

    private boolean updateTopicProducerWriteAcls(String topicName, AdminClient client,
                                              PatternType patternType, String host,
                                              String principal) throws InterruptedException, ExecutionException, TimeoutException {
        List<AclBinding> aclListArray = new ArrayList<>();

        ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, patternType);
        AccessControlEntry aclEntry = new AccessControlEntry(principal, host, AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding1 = new AclBinding(resourcePattern, aclEntry);
        aclListArray.add(aclBinding1);

        boolean acl1Exists = aclExists(client, aclBinding1.toFilter());

        aclEntry = new AccessControlEntry(principal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        AclBinding aclBinding2 = new AclBinding(resourcePattern, aclEntry);
        aclListArray.add(aclBinding2);

        boolean acl2Exists = aclExists(client, aclBinding2.toFilter());

        if(acl1Exists && acl2Exists){
            return true;
        }else {
            client.createAcls(aclListArray).all().get(timeOutSecsForAcls, TimeUnit.SECONDS);
            return false;
        }
    }

    private boolean aclExists(AdminClient client, AclBindingFilter aclBindingFilter){
        DescribeAclsResult aclsResult = client.describeAcls(aclBindingFilter);
        try {
            if(aclsResult.values().get(timeOutSecsForAcls, TimeUnit.SECONDS)
                    .size()==1)
                return true;
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void updateTransactionalIdAclsForProducer(String transactionalId, AdminClient client, PatternType patternType,
                                                      String host, String principal, String aclOperation) throws InterruptedException, ExecutionException, TimeoutException {

        List<AclBinding> aclListArray = new ArrayList<>();
        // Adding transactional id acls
        if (transactionalId != null)
            transactionalId = transactionalId.trim();

        if (transactionalId != null && transactionalId.length() > 0) {
            ResourcePattern resourcePatternTxn = new ResourcePattern(ResourceType.TRANSACTIONAL_ID,
                    transactionalId, patternType);
            AccessControlEntry aclEntryTxn = new AccessControlEntry(principal, host, AclOperation.WRITE, AclPermissionType.ALLOW);
            AclBinding aclBinding1Txn = new AclBinding(resourcePatternTxn, aclEntryTxn);
            aclListArray.add(aclBinding1Txn);

            if(aclOperation.equals("Create"))
                client.createAcls(aclListArray);
            else
            {
                List<AclBindingFilter> aclListArrayDel = new ArrayList<>();

                ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, transactionalId, patternType);
                AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal, host, AclOperation.WRITE, AclPermissionType.ALLOW);
                AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern,aclEntry);
                aclListArrayDel.add(aclBinding1);
                client.deleteAcls(aclListArrayDel).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
            }
        }
    }

    public synchronized String updateConsumerAcl(String topicName, String environment, String protocol, String clusterName,
                                                 String acl_ip, String acl_ssl, String consumerGroup, String aclOperation,
                                                 String isPrefixAcl, String aclIpPrincipleType) {
        log.info("updateConsumerAcl TopicName:{} Env:{} Protocol:{} AclIP:{} AclSSL:{} Consumergroup:{}" +
                        " AclOperation:{} PrefixAcl {} aclIpPrincipleType {}",
                topicName, environment, protocol, acl_ip,
                acl_ssl, consumerGroup, aclOperation, isPrefixAcl, aclIpPrincipleType);
        AdminClient client;
        String resultStr = "";
        try {
            PatternType patternType;
            patternType = PatternType.LITERAL;

            client = getAdminClient.getAdminClient(environment, protocol, clusterName);
            if(client == null)
                return "failure";

            String host = null, principal = null;
            boolean isValidParam = false;

            if(acl_ssl != null && acl_ssl.trim().length() > 0 && !acl_ssl.equals("User:*")){
                acl_ssl = acl_ssl.trim();

//                if(acl_ssl.toLowerCase().startsWith("cn"))
                if(aclIpPrincipleType.equals(AclIPPrincipleType.PRINCIPLE.name()))
                {
                    host = "*";
                    principal = "User:"+acl_ssl;
                    isValidParam = true;
                }

                if(aclOperation.equals("Create") && isValidParam){
                    List<AclBinding> aclListArray = new ArrayList<>();

                    AccessControlEntry aclEntry = new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW);
                    ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, patternType);

                    AclBinding aclBinding1 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    boolean acl1Exists = aclExists(client, aclBinding1.toFilter());

                    aclEntry = new AccessControlEntry(principal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding2);

                    boolean acl2Exists = aclExists(client, aclBinding2.toFilter());

                    resourcePattern = new ResourcePattern(ResourceType.GROUP, consumerGroup, patternType);
                    aclEntry = new AccessControlEntry(principal,host, AclOperation.READ, AclPermissionType.ALLOW);
                    AclBinding aclBinding3 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding3);

                    boolean acl3Exists = aclExists(client, aclBinding3.toFilter());
                    if(acl1Exists && acl2Exists && acl3Exists){
                        resultStr = "Acl already exists. success";
                    }else {
                        client.createAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                        resultStr = "success";
                    }

                }else if(isValidParam){
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host, AclOperation.READ, AclPermissionType.ALLOW);
                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, topicName, patternType);

                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePatternFilter(ResourceType.GROUP,consumerGroup, patternType);
                    aclEntry = new AccessControlEntryFilter(principal, host, AclOperation.READ, AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding3 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding3);

                    client.deleteAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                    resultStr = "success";
                }
            }

            if(acl_ip!=null && acl_ip.trim().length()>0){
                acl_ip = acl_ip.trim();

                    host = acl_ip;
                    principal = "User:*";

                if(aclOperation.equals("Create")){
                    List<AclBinding> aclListArray = new ArrayList<>();

                    ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, patternType);
                    AccessControlEntry aclEntry = new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW);
                    AclBinding aclBinding1 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    boolean acl1Exists = aclExists(client, aclBinding1.toFilter());

                    aclEntry = new AccessControlEntry(principal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding2);

                    boolean acl2Exists = aclExists(client, aclBinding2.toFilter());

                    resourcePattern = new ResourcePattern(ResourceType.GROUP, consumerGroup, patternType);
                    aclEntry = new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW);
                    AclBinding aclBinding3 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding3);

                    boolean acl3Exists = aclExists(client, aclBinding3.toFilter());

                    if(acl1Exists && acl2Exists && acl3Exists){
                        resultStr = "Acl already exists. success";
                    }else {
                        client.createAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                        resultStr = "success";
                    }

                }else {
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC,topicName, patternType);
                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePatternFilter(ResourceType.GROUP,consumerGroup, patternType);
                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding3 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding3);

                    client.deleteAcls(aclListArray).all().get(timeOutSecsForAcls,TimeUnit.SECONDS);
                    resultStr = "success";
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            return "failure";
        }

        return resultStr ;
    }

    public synchronized String postSchema(String topicName, String schema, String environmentVal){
            try {
                log.info("Into post schema request TopicName:{} Env:{}",topicName, environmentVal);
                if(environmentVal == null)
                    return "Cannot retrieve SchemaRegistry Url";

                // set default compatibility
                setSchemaCompatibility(environmentVal, topicName, false);

                String schemaRegistryUrl = "http://" + environmentVal;

                String uri = schemaRegistryUrl + "/subjects/" +
                        topicName + "-value/versions";
                RestTemplate restTemplate = getAdminClient.getRestTemplate();

                Map<String, String> params = new HashMap<String, String>();
                params.put("schema", schema);

                HttpHeaders headers = new HttpHeaders();
                headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");
                HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
                ResponseEntity<String> responseNew = restTemplate.postForEntity(uri, request, String.class);

                String updateTopicReqStatus = responseNew.getBody();
                log.info(responseNew.getBody());

                return updateTopicReqStatus;
            }
            catch(Exception e){
                log.error(e.getMessage());
                if(((HttpClientErrorException.Conflict) e).getStatusCode().value() == 409)
                {
                    return "Schema being registered is incompatible with an earlier schema";
                }
                return "Failure in registering schema.";
            }
    }

    public TreeMap<Integer, HashMap<String, Object>> getSchema(String environmentVal, String topicName){
        try {
            log.info("Into getSchema request {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return null;

            List<Integer> versionsList = getSchemaVersions(environmentVal, topicName);
            String schemaCompatibility = getSchemaCompatibility(environmentVal, topicName);
            String schemaRegistryUrl = "http://" + environmentVal;
            TreeMap<Integer, HashMap<String, Object>> allSchemaObjects = new TreeMap<>();

            for (Integer schemaVersion : versionsList) {
                String uri = schemaRegistryUrl + "/subjects/" +
                        topicName + "-value/versions/" + schemaVersion;
                RestTemplate restTemplate = getAdminClient.getRestTemplate();
                Map<String, String> params = new HashMap<String, String>();

                ResponseEntity<HashMap> responseNew = restTemplate.getForEntity(uri, HashMap.class, params);
                HashMap<String, Object> schemaResponse = responseNew.getBody();
                if(schemaResponse != null)
                    schemaResponse.put("compatibility", schemaCompatibility);

                log.info(Objects.requireNonNull(responseNew.getBody()).toString());
                allSchemaObjects.put(schemaVersion, schemaResponse);
            }

            return allSchemaObjects;
        }catch (Exception e)
        {
            log.error("Error from getSchema : " + e.getMessage());
            return new TreeMap<>();
        }
    }

    private List<Integer> getSchemaVersions(String environmentVal, String topicName){
        try {
            log.info("Into getSchema versions {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return null;
            String schemaRegistryUrl = "http://" + environmentVal;

            String uri = schemaRegistryUrl + "/subjects/" +
                    topicName + "-value/versions";
            RestTemplate restTemplate = getAdminClient.getRestTemplate();
            Map<String, String> params = new HashMap<String, String>();

            ResponseEntity<ArrayList> responseList = restTemplate.getForEntity(uri, ArrayList.class, params);
            log.info("Schema versions " + responseList);
            return responseList.getBody();
        }catch (Exception e)
        {
            log.error("Error in getting versions " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private String getSchemaCompatibility(String environmentVal, String topicName){
        try {
            log.info("Into getSchema compatibility {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return null;
            String schemaRegistryUrl = "http://" + environmentVal;

            String uri = schemaRegistryUrl + "/config/" +
                    topicName + "-value";
            RestTemplate restTemplate = getAdminClient.getRestTemplate();
            Map<String, String> params = new HashMap<String, String>();

            ResponseEntity<HashMap> responseList = restTemplate.getForEntity(uri, HashMap.class, params);
            log.info("Schema compatibility " + responseList);
            return (String)responseList.getBody().get("compatibilityLevel");
        }catch (Exception e)
        {
            log.error("Error in getting schema compatibility " + e.getMessage());
            return "NOT SET";
        }
    }

    private boolean setSchemaCompatibility(String environmentVal, String topicName, boolean isForce){
        try {
            log.info("Into setSchema compatibility {} {}", topicName, environmentVal);
            if (environmentVal == null)
                return false;
            String schemaRegistryUrl = "http://" + environmentVal;

            String uri = schemaRegistryUrl + "/config/" +
                    topicName + "-value";
            RestTemplate restTemplate = getAdminClient.getRestTemplate();

            Map<String, String> params = new HashMap<>();
            if(isForce)
                params.put("compatibility", "NONE");
            else
                params.put("compatibility", defaultSchemaCompatibility);

            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");
            HttpEntity<Map<String, String>> request = new HttpEntity<>(params, headers);
            restTemplate.put(uri, request, String.class);
            return true;
        }catch (Exception e)
        {
            log.error("Error in setting schema compatibility " + e.getMessage());
            return false;
        }
    }
}
