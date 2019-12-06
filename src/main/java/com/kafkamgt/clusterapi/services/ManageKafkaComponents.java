package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.utils.GetAdminClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class ManageKafkaComponents {

    private static Logger LOG = LoggerFactory.getLogger(ManageKafkaComponents.class);

    @Autowired
    Environment env;

    @Autowired
    GetAdminClient getAdminClient;

    public String getStatus(String environment){

        try {
            AdminClient client = getAdminClient.getAdminClient(environment);
            if(client.listTopics().names().get().size()>=0)
                return "ONLINE";
        } catch (InterruptedException e) {
            e.printStackTrace();
            return "OFFLINE";
        } catch (ExecutionException e) {
            e.printStackTrace();
            return "OFFLINE";
        }
        catch (Exception e){
            return "OFFLINE";
        }

        return "OFFLINE";
    }

    public Set<HashMap<String,String>> loadAcls(String environment){
        Set<HashMap<String,String>> acls = new HashSet<>();

        AdminClient client = getAdminClient.getAdminClient(environment);

        AclBindingFilter aclBindingFilter = AclBindingFilter.ANY;
        DescribeAclsResult s = client.describeAcls(aclBindingFilter);

         try {
            s.values().get().stream().forEach(aclBinding -> {
                //LOG.info(aclBinding+" ---- aclBinding");
                HashMap<String,String> aclbindingMap = new HashMap<>();
                aclbindingMap.put("host",aclBinding.entry().host());
                aclbindingMap.put("principle",aclBinding.entry().principal());
                aclbindingMap.put("operation",aclBinding.entry().operation().toString());
                aclbindingMap.put("permissionType",aclBinding.entry().permissionType().toString());
                aclbindingMap.put("resourceType",aclBinding.pattern().resourceType().toString());
                aclbindingMap.put("resourceName",aclBinding.pattern().name());

                if(!aclBinding.pattern().resourceType().toString().equals("CLUSTER")) {
                    if(aclBinding.entry().operation().toString().equals("WRITE") ||
                            aclBinding.entry().operation().toString().equals("READ"))
                    acls.add(aclbindingMap);
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return acls;
    }

        public Set<String> loadTopics(String environment){
            AdminClient client = getAdminClient.getAdminClient(environment);
            ListTopicsResult topicsResult = client.listTopics();
            Set<String> topics = new HashSet<>();
            try {

                DescribeTopicsResult s = client.describeTopics(new ArrayList<>(topicsResult.names().get()));
                Map<String, TopicDescription> topicDesc  = s.all().get();
                Set<String> keySet = topicDesc.keySet();
                List<String> lstK = new ArrayList<>(keySet);
                lstK.stream()
                        .forEach(topicName-> {
                            topics.add(topicName+":::::"+topicDesc.get(topicName).partitions().get(0).replicas().size()+
                                    ":::::"+topicDesc.get(topicName).partitions().size());
                                }
                        );

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            client.close();

            return topics;

        }

    public String createTopic(String name, String partitions, String replicationFactor, String environment) {

        LOG.info(name + "--"+partitions + "--"+replicationFactor + "--" + environment);

        try (AdminClient client = getAdminClient.getAdminClient(environment)) {

            NewTopic topic = new NewTopic(name, Integer.parseInt(partitions), Short.parseShort(replicationFactor));

            CreateTopicsResult result = client.createTopics(Collections.singletonList(topic));
            result.values().get(name).get();

        } catch (KafkaException e) {
            String errorMessage = "Invalid properties: ";
            LOG.error(errorMessage, e);
            throw e;
        } catch (NumberFormatException e) {
            String errorMessage = "Invalid replica assignment string";
            LOG.error(errorMessage, e);
            throw e;
        } catch (ExecutionException | InterruptedException e) {
            String errorMessage;
            if (e instanceof ExecutionException) {
                errorMessage = e.getCause().getMessage();
            } else {
                Thread.currentThread().interrupt();
                errorMessage = e.getMessage();
            }
            LOG.error("Unable to create topic {}", name, e);
        }

        //createProducerAcl(name,environment,acl_ip,acl_ssl);
        return "success";

    }

    public String createProducerAcl(String topicName, String environment, String acl_ip, String acl_ssl) {

        LOG.info("In producer alcs::"+acl_ip +"--"+ acl_ssl);

        try (AdminClient client = getAdminClient.getAdminClient(environment)) {
            List<AclBinding> aclListArray = new ArrayList<AclBinding>();
            String host = null, principal=null;
            if(acl_ssl!=null  && acl_ssl.trim().length()>0){
                acl_ssl=acl_ssl.trim();
                if(acl_ssl.contains("CN") || acl_ssl.contains("cn"))
                {
                    host = "*";
                    principal = "User:"+acl_ssl;

                    LOG.info(principal+"In producer alcs::"+host);

                    Resource resource = new Resource(ResourceType.TOPIC,topicName);
                    AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                    AclBinding aclBinding1 = new AclBinding(resource,aclEntry);
                    aclListArray.add(aclBinding1);

                    resource = new Resource(ResourceType.TOPIC,topicName);
                    aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resource,aclEntry);
                    aclListArray.add(aclBinding2);

                    LOG.info(aclListArray.get(0).entry().host()+"----"+aclListArray.get(0).entry().principal());
                    client.createAcls(aclListArray);
                }

                client.close();
            }

            if(acl_ip!=null && acl_ip.trim().length()>0){
                acl_ip=acl_ip.trim();
                if(acl_ip!=null)
                {
                    host=acl_ip;
                    principal="User:*";

                    LOG.info(principal+"In producer alcs::"+host);

                    Resource resource = new Resource(ResourceType.TOPIC,topicName);
                    AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                    AclBinding aclBinding1 = new AclBinding(resource,aclEntry);
                    aclListArray.add(aclBinding1);

                    resource = new Resource(ResourceType.TOPIC,topicName);
                    aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                    AclBinding aclBinding2 = new AclBinding(resource,aclEntry);
                    aclListArray.add(aclBinding2);

                    LOG.info(aclListArray.get(0).entry().host()+"----"+aclListArray.get(0).entry().principal());
                    client.createAcls(aclListArray);
                    client.close();
                }

            }
        }
        return "success";
        }

    public String createConsumerAcl(String topicName, String environment, String acl_ip, String acl_ssl, String consumerGroup) {

        try (AdminClient client = getAdminClient.getAdminClient(environment)) {
            List<AclBinding> aclListArray = new ArrayList<AclBinding>();
            String host = null, principal=null;

            LOG.info(acl_ssl+"----acl_ssl");
            if(acl_ssl!=null && acl_ssl.trim().length()>0){
                acl_ssl=acl_ssl.trim();
                if(acl_ssl.contains("CN") || acl_ssl.contains("cn"))
                {
                    host = "";
                    principal = "User:"+acl_ssl;
                }


                Resource resource = new Resource(ResourceType.TOPIC,topicName);
                AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                AclBinding aclBinding1 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding1);

                resource = new Resource(ResourceType.TOPIC,topicName);
                aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                AclBinding aclBinding2 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding2);

                resource = new Resource(ResourceType.GROUP,consumerGroup);
                aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                AclBinding aclBinding3 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding3);

                LOG.info(aclListArray.get(0).entry().host()+"----");
                client.createAcls(aclListArray);
            }

            if(acl_ip!=null && acl_ip.trim().length()>0){
                acl_ip=acl_ip.trim();

                    host=acl_ip;
                    principal="User:*";

                Resource resource = new Resource(ResourceType.TOPIC,topicName);
                AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                AclBinding aclBinding1 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding1);

                resource = new Resource(ResourceType.TOPIC,topicName);
                aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                AclBinding aclBinding2 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding2);

                resource = new Resource(ResourceType.GROUP,consumerGroup);
                aclEntry = new AccessControlEntry(principal,host,AclOperation.READ,AclPermissionType.ALLOW);
                AclBinding aclBinding3 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding3);

                LOG.info(aclListArray.get(0).entry().host()+"----");
                client.createAcls(aclListArray);
            }
        }
        return "success";
    }

    public String postSchema(String topicName, String schema, String environmentVal){
            try {
                String uri = env.getProperty(environmentVal+".schemaregistry.url") + "/subjects/" + topicName + "-value/versions";
                RestTemplate restTemplate = new RestTemplate();

                Map<String, String> params = new HashMap<String, String>();

                params.put("schema", schema);

                HttpHeaders headers = new HttpHeaders();//createHeaders("user1", "pwd");
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
