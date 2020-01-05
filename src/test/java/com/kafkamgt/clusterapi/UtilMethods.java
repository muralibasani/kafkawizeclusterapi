package com.kafkamgt.clusterapi;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;

public class UtilMethods {
    public List<AclBinding> getListAclBindings(AccessControlEntry accessControlEntry){
        List<AclBinding> listAclBinding = new ArrayList<>();
        AclBinding aclBinding = new AclBinding
                (new ResourcePattern(ResourceType.GROUP,
                        "consgroup1",
                        PatternType.LITERAL), accessControlEntry);
        listAclBinding.add(aclBinding);

        return listAclBinding;
    }

    public Set<HashMap<String,String>> getAcls(){
        Set<HashMap<String,String>> aclsSet = new HashSet<>();
        HashMap<String, String> hMap = new HashMap<>();
        hMap.put("host", "12.11.124.11");
        hMap.put("principle", "User:*");
        hMap.put("operation", "READ");
        hMap.put("permissionType", "ALLOW");
        hMap.put("resourceType", "GROUP");
        hMap.put("resourceName", "consumergroup1");

        aclsSet.add(hMap);

        hMap = new HashMap<>();
        hMap.put("host", "12.15.124.12");
        hMap.put("principle", "User:*");
        hMap.put("operation", "READ");
        hMap.put("permissionType", "ALLOW");
        hMap.put("resourceType", "TOPIC");
        hMap.put("resourceName", "testtopic");
        aclsSet.add(hMap);

        return aclsSet;
    }

    public Set<String> getTopics() {
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add("testtopic1");
        return topicsSet;
    }

    public MultiValueMap<String, String> getMappedValuesTopic(){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("env","localhost");
        params.add("topicName", "testtopic");
        params.add("partitions", "2");
        params.add("rf", "1");

        return params;
    }

    public MultiValueMap<String, String> getMappedValuesAcls(String aclType){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("env","localhost");
        params.add("topicName", "testtopic");
        params.add("consumerGroup", "congroup1");
        params.add("aclType", aclType);
        params.add("acl_ip", "11.12.33.122");
        params.add("acl_ssl", null);

        return params;
    }

    public MultiValueMap<String, String> getMappedValuesSchema(){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("env","localhost");
        params.add("topicName", "testtopic");
        params.add("fullSchema", "{type:string}");

        return params;
    }
}
