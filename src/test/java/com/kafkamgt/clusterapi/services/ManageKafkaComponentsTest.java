package com.kafkamgt.clusterapi.services;

import com.kafkamgt.clusterapi.UtilMethods;
import com.kafkamgt.clusterapi.utils.AdminClientUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ManageKafkaComponentsTest {

    @Mock
    private AdminClientUtils getAdminClient;

    @Mock
    private Environment env;

    @Mock
    private AdminClient adminClient;

    @Mock
    private ListTopicsResult listTopicsResult;

    @Mock
    private KafkaFuture<Set<String>> kafkaFuture;

    @Mock
    private KafkaFuture<Map<String, TopicDescription>> kafkaFutureTopicdesc;

    @Mock
    private KafkaFuture<Collection<AclBinding>> kafkaFutureCollection;

    @Mock
    private DescribeTopicsResult describeTopicsResult;

    @Mock
    private DescribeAclsResult describeAclsResult;

    @Mock
    private AccessControlEntry accessControlEntry;

    @Mock
    private CreateTopicsResult createTopicsResult;

    @Mock
    private CreateAclsResult createAclsResult;

    @Mock
    private Map<String, KafkaFuture<Void>> futureTocpiCreateResult;

    @Mock
    private KafkaFuture<Void> kFutureVoid;

    @Mock
    private RestTemplate restTemplate;

    private UtilMethods utilMethods;

    private ManageKafkaComponents manageKafkaComponents;

    @Before
    public void setUp() {
        manageKafkaComponents = new ManageKafkaComponents(env, getAdminClient);
        utilMethods = new UtilMethods();
    }

    @Test
    public void getStatusOnline() throws ExecutionException, InterruptedException {
        Set<HashMap<String,String>> topicsSet = utilMethods.getTopics();

        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
//        when(adminClient.listTopics()).thenReturn(listTopicsResult);
//        when(listTopicsResult.names()).thenReturn(kafkaFuture);
//        doNothing().when(kafkaFuture.get());

        String result = manageKafkaComponents.getStatus("localhost");
        assertEquals("ONLINE", result);
    }

    @Test
    public void getStatusOffline1(){
        when(getAdminClient.getAdminClient(any())).thenReturn(null);
        String result = manageKafkaComponents.getStatus("localhost");
        assertEquals("OFFLINE", result);
    }

    @Test
    public void getStatusOffline2(){
        when(getAdminClient.getAdminClient(any())).thenThrow(new RuntimeException("Error"));
        String result = manageKafkaComponents.getStatus("localhost");
        assertEquals("OFFLINE", result);
    }

    @Test
    public void loadAcls1() throws ExecutionException, InterruptedException {
        List<AclBinding> listAclBindings = utilMethods.getListAclBindings(accessControlEntry);

        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(kafkaFutureCollection);
        when(kafkaFutureCollection.get()).thenReturn(listAclBindings);
        when(accessControlEntry.host()).thenReturn("11.12.33.456");
        when(accessControlEntry.operation()).thenReturn(AclOperation.READ);
        when(accessControlEntry.permissionType()).thenReturn(AclPermissionType.ALLOW);


        Set<HashMap<String,String>> result = manageKafkaComponents.loadAcls("localhost");
        assertEquals(1, result.size());
    }

    @Test
    public void loadAcls2() throws ExecutionException, InterruptedException {
        List<AclBinding> listAclBindings = utilMethods.getListAclBindings(accessControlEntry);

        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenReturn(describeAclsResult);
        when(describeAclsResult.values()).thenReturn(kafkaFutureCollection);
        when(kafkaFutureCollection.get()).thenReturn(listAclBindings);
        when(accessControlEntry.host()).thenReturn("11.12.33.456");
        when(accessControlEntry.operation()).thenReturn(AclOperation.CREATE);
        when(accessControlEntry.permissionType()).thenReturn(AclPermissionType.ALLOW);


        Set<HashMap<String,String>> result = manageKafkaComponents.loadAcls("localhost");
        assertEquals(0, result.size());
    }

    @Test
    public void loadAcls3() {
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.describeAcls(any())).thenThrow(new RuntimeException("Describe Acls Error"));

        Set<HashMap<String,String>> result = manageKafkaComponents.loadAcls("localhost");
        assertEquals(0, result.size());
    }

    @Test
    public void loadTopics() throws ExecutionException, InterruptedException {
        Set<HashMap<String,String>> topicsSet = utilMethods.getTopics();
        Set<String> list = new HashSet<>();
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(kafkaFuture);
        when(kafkaFuture.get()).thenReturn(list);

        when(adminClient.describeTopics(any())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.all()).thenReturn(kafkaFutureTopicdesc);
        when(kafkaFutureTopicdesc.get()).thenReturn(getTopicDescs());

        Set<HashMap<String,String>> result = manageKafkaComponents.loadTopics("localhost");

        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("partitions","2");
        hashMap.put("replicationFactor","1");
        hashMap.put("topicName","testtopic2");

        HashMap<String, String> hashMap1 = new HashMap<>();
        hashMap1.put("partitions","2");
        hashMap1.put("replicationFactor","1");
        hashMap1.put("topicName","testtopic1");

        assertEquals(2, result.size());
        assertEquals(hashMap, new ArrayList<>(result).get(0));
        assertEquals(hashMap1, new ArrayList<>(result).get(1));
    }

    @Test
    public void createTopicSuccess() throws Exception {
        String name = "testtopic1", partitions = "1", replicationFactor = "1", environment = "localhost";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(futureTocpiCreateResult);
        when(futureTocpiCreateResult.get(anyString())).thenReturn(kFutureVoid);

        String result = manageKafkaComponents.createTopic(name, partitions, replicationFactor,
                environment);
        assertEquals("success", result);
    }

    @Test(expected = Exception.class)
    public void createTopicFailure1() throws Exception {
        String name = "testtopic1", partitions = "1", replicationFactor = "1", environment = "localhost";
        when(getAdminClient.getAdminClient(any())).thenReturn(null);

        manageKafkaComponents.createTopic(name, partitions, replicationFactor,
                environment);
    }

    @Test(expected = NumberFormatException.class)
    public void createTopicFailure2() throws Exception {
        String name = "testtopic1", partitions = "1aa", replicationFactor = "1aa", environment = "localhost";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);

        manageKafkaComponents.createTopic(name, partitions, replicationFactor,
                environment);
    }

    @Test(expected = RuntimeException.class)
    public void createTopicFailure4() throws Exception {
        String name = "testtopic1", partitions = "1", replicationFactor = "1", environment = "localhost";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createTopics(any())).thenThrow(new RuntimeException("Runtime exption"));

        manageKafkaComponents.createTopic(name, partitions, replicationFactor,
                environment);
    }

    @Test
    public void createProducerAcl1() {
        String topicName = "testtopic",  environment = "localhost",
                 acl_ip = "110.11.21.112";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createAcls(any())).thenReturn(createAclsResult);

        String result = manageKafkaComponents.updateProducerAcl(topicName, environment,
                acl_ip, null,"Create");
        assertEquals("success", result);
    }

    @Test
    public void createProducerAcl2() {
        String topicName = "testtopic",  environment = "localhost",
                  acl_ssl = "CN=host,OU=...";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createAcls(any())).thenReturn(createAclsResult);

        String result = manageKafkaComponents.updateProducerAcl(topicName, environment,
                null, acl_ssl,"Create");
        assertEquals("success", result);
    }

    @Test
    public void createConsumerAcl1() {
        String topicName = "testtopic",  environment = "localhost",
                acl_ip = "110.11.21.112", consumerGroup="congroup1";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createAcls(any())).thenReturn(createAclsResult);

        String result = manageKafkaComponents.updateConsumerAcl(topicName, environment,
                acl_ip, null, consumerGroup,"Create");
        assertEquals("success", result);
    }

    @Test
    public void createConsumerAcl2() {
        String topicName = "testtopic",  environment = "localhost",
                  acl_ssl = "CN=host,OU=...", consumerGroup="congroup1";
        when(getAdminClient.getAdminClient(any())).thenReturn(adminClient);
        when(adminClient.createAcls(any())).thenReturn(createAclsResult);

        String result = manageKafkaComponents.updateConsumerAcl(topicName, environment,
                null, acl_ssl, consumerGroup,"Create");
        assertEquals("success", result);
    }

    @Test
    public void postSchema1() {
        String topicName="testtopic1", schema="{type:string}", environmentVal="localhost";
        ResponseEntity<String> response = new ResponseEntity<>("Schema created id : 101",
                HttpStatus.OK);

        when(env.getProperty(environmentVal +".schemaregistry.url"))
                .thenReturn("http://localhost:8081");
        when(getAdminClient.getRestTemplate()).thenReturn(restTemplate);
        when(restTemplate.postForEntity
                (anyString(), any(),
                        eq(String.class)))
                .thenReturn(response);

        String result = manageKafkaComponents.postSchema(topicName, schema, environmentVal);
        assertEquals("Schema created id : 101", result);
    }

    @Test
    public void postSchema2() {
        String topicName="testtopic1", schema="{type:string}", environmentVal="localhost";
        when(env.getProperty(environmentVal +".schemaregistry.url")).thenReturn(null);

        String result = manageKafkaComponents.postSchema(topicName, schema, environmentVal);
        assertEquals("Cannot retrieve SchemaRegistry Url", result);
    }

    private Map<String, TopicDescription> getTopicDescs(){
        Node node = new Node(1,"localhost",1);

        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(2, node,
                Arrays.asList(node),  Arrays.asList(node));
        TopicDescription tDesc = new TopicDescription("testtopic", true,
                Arrays.asList(topicPartitionInfo, topicPartitionInfo));
        Map<String, TopicDescription> mapResults = new HashMap<>();
        mapResults.put("testtopic1",tDesc);

        tDesc = new TopicDescription("testtopic2", true,
                Arrays.asList(topicPartitionInfo, topicPartitionInfo));
        mapResults.put("testtopic2",tDesc);

        return mapResults;
    }


}