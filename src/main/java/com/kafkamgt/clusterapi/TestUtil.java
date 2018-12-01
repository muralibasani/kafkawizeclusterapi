package com.kafkamgt.clusterapi;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class TestUtil {

    private static Logger LOG = LoggerFactory.getLogger(TestUtil.class);


    public TestUtil(){

        createProducerAcl("testtopic175","","19.10.10.101");
        createProducerAcl("testtopic175","","CN=kafka-dev-0.europe.intranet,OU=Internal Services,OU=PKI,OU=Services,O=ING");
    }



    public String createProducerAcl(String topicName, String environment, String acl) {

        Properties props = new Properties();
        LOG.info("In producer alcs::"+acl);
        props.put("bootstrap.servers","kafka-dev-0.europe.intranet:9092");

        try (AdminClient client = AdminClient.create(props)) {
            List<AclBinding> aclListArray = new ArrayList<AclBinding>();
            String host = null, principal=null;
            if(acl!=null){
                acl=acl.trim();
                if(acl.contains("CN") || acl.contains("cn"))
                {
                    host = "*";
                    principal = "User:"+acl;
                }
                else{
                    host=acl;
                    principal="User:*";
                }
                LOG.info(principal+"----In producer alcs::"+host);

                Resource resource = new Resource(ResourceType.TOPIC,topicName);
                AccessControlEntry aclEntry = new AccessControlEntry(principal,host,AclOperation.WRITE,AclPermissionType.ALLOW);
                AclBinding aclBinding1 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding1);

                resource = new Resource(ResourceType.TOPIC,topicName);
                aclEntry = new AccessControlEntry(principal,host,AclOperation.DESCRIBE,AclPermissionType.ALLOW);
                AclBinding aclBinding2 = new AclBinding(resource,aclEntry);
                aclListArray.add(aclBinding2);


//                LOG.info(aclListArray.get(0).entry().host()+"----"+aclListArray.get(0).entry().principal());
                CreateAclsResult s =  client.createAcls(aclListArray);
                LOG.info("---"+s.values().toString());

//                try {
//                    LOG.info("+++++++++++++++++"+client.describeAcls(aclBinding1.toFilter()).values().get().iterator().next().toString());
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }

                client.close();


            }
        }
        return "success";
    }



//    public static void main(String[] args){
//        new TestUtil();
//    }
}
