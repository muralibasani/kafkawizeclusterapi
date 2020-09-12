package com.kafkamgt.clusterapi.services;

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
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class ManageKafkaComponents {

    @Autowired
    Environment env;

    @Autowired
    AdminClientUtils getAdminClient;

    public ManageKafkaComponents(){}

    public ManageKafkaComponents(Environment env, AdminClientUtils getAdminClient){
        this.env = env;
        this.getAdminClient = getAdminClient;
    }

    public String getStatus(String environment){

        try {
            AdminClient client = getAdminClient.getAdminClient(environment);
            if(client != null)
                return "ONLINE";
            else
                return "OFFLINE";
        } catch (Exception e){
            return "OFFLINE";
        }
    }

    public Set<HashMap<String,String>> loadAcls(String environment){
        Set<HashMap<String,String>> acls = new HashSet<>();

        AdminClient client = getAdminClient.getAdminClient(environment);
        if(client == null)
            return acls;

         try {
             AclBindingFilter aclBindingFilter = AclBindingFilter.ANY;
             DescribeAclsResult aclsResult = client.describeAcls(aclBindingFilter);

                aclsResult.values().get().stream()
                    .forEach(aclBinding -> {
                HashMap<String,String> aclbindingMap = new HashMap<>();
                aclbindingMap.put("host", aclBinding.entry().host());
                aclbindingMap.put("principle", aclBinding.entry().principal());
                aclbindingMap.put("operation", aclBinding.entry().operation().toString());
                aclbindingMap.put("permissionType", aclBinding.entry().permissionType().toString());
                aclbindingMap.put("resourceType", aclBinding.pattern().resourceType().toString());
                aclbindingMap.put("resourceName", aclBinding.pattern().name());

                if(!aclBinding.pattern().resourceType().toString().equals("CLUSTER")) {
                    if(aclBinding.entry().operation().toString().equals("WRITE") ||
                            aclBinding.entry().operation().toString().equals("READ"))
                    acls.add(aclbindingMap);
                }
            });
        }catch (Exception e){
             log.error("Error "+e.getMessage());
             e.printStackTrace();
         }

        client.close();
        return acls;
    }

    public Set<HashMap<String,String>> loadTopics(String environment){
        AdminClient client = getAdminClient.getAdminClient(environment);
        Set<HashMap<String,String>> topics = new HashSet<>();
        if(client == null)
            return topics;

        ListTopicsResult topicsResult = client.listTopics();

        try {
            DescribeTopicsResult s = client.describeTopics(new ArrayList<>(topicsResult.names().get()));
            Map<String, TopicDescription> topicDesc  = s.all().get();
            Set<String> keySet = topicDesc.keySet();
            List<String> lstK = new ArrayList<>(keySet);
            HashMap<String,String> hashMap;
            for (String topicName : lstK) {
                hashMap = new HashMap<>();
                hashMap.put("topicName",topicName);
                hashMap.put("replicationFactor","" + topicDesc.get(topicName).partitions().get(0).replicas().size());
                hashMap.put("partitions", "" + topicDesc.get(topicName).partitions().size());
                topics.add(hashMap);
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        client.close();

        return topics;
    }

    public String createTopic(String name, String partitions, String replicationFactor,
                              String environment) throws Exception {

        log.info(name + "--"+partitions + "--"+replicationFactor + "--" + environment);

        AdminClient client;
        try{
            client = getAdminClient.getAdminClient(environment);
            if(client == null)
                throw new Exception("Cannot connect to cluster.");

            NewTopic topic = new NewTopic(name, Integer.parseInt(partitions),
                    Short.parseShort(replicationFactor));

            CreateTopicsResult result = client.createTopics(Collections.singletonList(topic));
            result.values().get(name).get();
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
            throw e;
        }

        client.close();
        return "success";

    }

    public void deleteTopic(String topicName, String environment) throws Exception {

        log.info(topicName + "--" + environment);

        AdminClient client;
        try{
            client = getAdminClient.getAdminClient(environment);
            if(client == null)
                throw new Exception("Cannot connect to cluster.");

            DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topicName));
            result.values().get(topicName).get();
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
            throw e;
        }

        client.close();
    }

    public String updateProducerAcl(String topicName, String environment,
                                    String acl_ip, String acl_ssl, String aclOperation) {

        log.info("In producer alcs::"+acl_ip +"--"+ acl_ssl);
        AdminClient client;
        try {
            client = getAdminClient.getAdminClient(environment);
            if(client==null)
               return "failure";

            String host, principal;
            if(acl_ssl != null  && acl_ssl.trim().length()>0){
                acl_ssl = acl_ssl.trim();
                if(acl_ssl.contains("CN") || acl_ssl.contains("cn"))
                {
                    host = "*";
                    principal = "User:"+acl_ssl;

                    log.info(principal+"In producer alcs::"+host);

                    if(aclOperation.equals("Create")){
                        List<AclBinding> aclListArray = new ArrayList<>();

                        ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                        AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                        AclBinding aclBinding1 = new AclBinding(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding1);

                        aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                        AclBinding aclBinding2 = new AclBinding(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding2);

                        log.info("Create -- "+aclListArray.get(0).entry().host()+"----"+aclListArray.get(0).entry().principal());
                        client.createAcls(aclListArray);
                    }else{
                        List<AclBindingFilter> aclListArray = new ArrayList<>();

                        ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                        AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                        AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding1);

                        aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                        AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                        aclListArray.add(aclBinding2);

                        log.info("Delete -- "+aclListArray.get(0).entryFilter().host()+"----"+aclListArray.get(0).entryFilter().principal());
                        client.deleteAcls(aclListArray);
                    }

                }
            }

            if(acl_ip != null && acl_ip.trim().length()>0){
                acl_ip = acl_ip.trim();

                host=acl_ip;
                principal="User:*";

                log.info(principal+"In producer alcs::"+host);

                if(aclOperation.equals("Create")){
                    List<AclBinding> aclListArray = new ArrayList<>();

                    ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                    AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                    AclBinding aclBinding1 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding2);

                    log.info(aclListArray.get(0).entry().host()+"----"+aclListArray.get(0).entry().principal());
                    client.createAcls(aclListArray);
                }else{
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding2);

                    log.info(aclListArray.get(0).entryFilter().host()+"----"+aclListArray.get(0).entryFilter().principal());
                    client.deleteAcls(aclListArray);
                }
            }

        }catch (Exception e){
            return "failure";
        }

        client.close();
        return "success";
        }

    public String updateConsumerAcl(String topicName, String environment, String acl_ip,
                                    String acl_ssl, String consumerGroup, String aclOperation) {
        AdminClient client;
        try {
            client = getAdminClient.getAdminClient(environment);
            if(client==null)
                return "failure";

            String host = null, principal=null;

            log.info(acl_ssl+"----acl_ssl");
            if(acl_ssl!=null && acl_ssl.trim().length()>0){
                acl_ssl=acl_ssl.trim();
                if(acl_ssl.contains("CN") || acl_ssl.contains("cn"))
                {
                    host = "*";
                    principal = "User:"+acl_ssl;
                }


                if(aclOperation.equals("Create")){
                    List<AclBinding> aclListArray = new ArrayList<>();

                    AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL);

                    AclBinding aclBinding1 = new AclBinding(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePattern(ResourceType.GROUP,consumerGroup,PatternType.LITERAL);
                    aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBinding aclBinding3 = new AclBinding(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding3);

                    log.info(aclListArray.get(0).entry().host()+"----");
                    client.createAcls(aclListArray);
                }else{
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC,topicName,PatternType.LITERAL);

                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern, aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePatternFilter(ResourceType.GROUP,consumerGroup,PatternType.LITERAL);
                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding3 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding3);

                    log.info(aclListArray.get(0).entryFilter().host()+"----");
                    client.deleteAcls(aclListArray);
                }
            }

            if(acl_ip!=null && acl_ip.trim().length()>0){
                acl_ip = acl_ip.trim();

                    host = acl_ip;
                    principal = "User:*";

                if(aclOperation.equals("Create")){
                    List<AclBinding> aclListArray = new ArrayList<>();

                    ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                    AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBinding aclBinding1 = new AclBinding(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePattern(ResourceType.GROUP,consumerGroup,PatternType.LITERAL);
                    aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBinding aclBinding3 = new AclBinding(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding3);

                    log.info(aclListArray.get(0).entry().host()+"----");
                    client.createAcls(aclListArray);
                    client.close();
                }else {
                    List<AclBindingFilter> aclListArray = new ArrayList<>();

                    ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC,topicName,PatternType.LITERAL);
                    AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding1 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding1);

                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding2 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding2);

                    resourcePattern = new ResourcePatternFilter(ResourceType.GROUP,consumerGroup,PatternType.LITERAL);
                    aclEntry = new AccessControlEntryFilter(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                    AclBindingFilter aclBinding3 = new AclBindingFilter(resourcePattern,aclEntry);
                    aclListArray.add(aclBinding3);

                    log.info(aclListArray.get(0).entryFilter().host()+"----");
                    client.deleteAcls(aclListArray);
                    client.close();
                }

            }
        }catch (Exception e){
            return "failure";
        }

        client.close();
        return "success";
    }

    public String postSchema(String topicName, String schema, String environmentVal){
            try {
                String schemaRegistryUrl = env.getProperty(environmentVal+".schemaregistry.url");
                if(schemaRegistryUrl == null)
                    return "Cannot retrieve SchemaRegistry Url";
                String uri = schemaRegistryUrl + "/subjects/" +
                        topicName + "-value/versions";
                RestTemplate restTemplate = getAdminClient.getRestTemplate();

                Map<String, String> params = new HashMap<String, String>();

                params.put("schema", schema);

                HttpHeaders headers = new HttpHeaders();
                headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");

                HttpEntity<Map<String, String>> request = new HttpEntity<Map<String, String>>(params, headers);

                ResponseEntity<String> responseNew = restTemplate.postForEntity(uri, request, String.class);

                String updateTopicReqStatus = responseNew.getBody();

                return updateTopicReqStatus;

            }
            catch(Exception e){
                return e.getMessage();
            }
    }

}
