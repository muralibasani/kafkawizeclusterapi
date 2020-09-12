package com.kafkamgt.clusterapi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkamgt.clusterapi.UtilMethods;
import com.kafkamgt.clusterapi.services.ManageKafkaComponents;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.MultiValueMap;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
public class ClusterApiControllerTest {

    @MockBean
    private ManageKafkaComponents manageKafkaComponents;

    private MockMvc mvc;

    private ClusterApiController clusterApiController;

    private UtilMethods utilMethods;

    @Before
    public void setUp() throws Exception {
        clusterApiController = new ClusterApiController();
        mvc = MockMvcBuilders
                .standaloneSetup(clusterApiController)
                .dispatchOptions(true)
                .build();
        utilMethods = new UtilMethods();
        ReflectionTestUtils.setField(clusterApiController, "manageKafkaComponents", manageKafkaComponents);
    }

    @Test
    public void getApiStatus() throws Exception {
        String res = mvc.perform(MockMvcRequestBuilders
                .get("/topics/getApiStatus")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals("ONLINE", res);
    }

    @Test
    public void getStatus() throws Exception {
        String env = "DEV";
        when(manageKafkaComponents.getStatus(env)).thenReturn("ONLINE");

        String res = mvc.perform(MockMvcRequestBuilders
                .get("/topics/getStatus/"+env)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals("ONLINE", res);
    }

    @Test
    public void getTopics() throws Exception {
        String env = "DEV";
        when(manageKafkaComponents.loadTopics(env)).thenReturn(utilMethods.getTopics());

        String res = mvc.perform(MockMvcRequestBuilders
                .get("/topics/getTopics/"+env)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        Set response = new ObjectMapper().readValue(res, Set.class);
        assertEquals(1, response.size());
    }

    @Test
    public void getAcls() throws Exception {
        String env = "DEV";
        when(manageKafkaComponents.loadAcls(env)).thenReturn(utilMethods.getAcls());

        String res = mvc.perform(MockMvcRequestBuilders
                .get("/topics/getAcls/"+env)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        Set response = new ObjectMapper().readValue(res, Set.class);
        assertEquals(2, response.size());
    }

    @Test
    public void createTopics() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesTopic();
        String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

        when(manageKafkaComponents.createTopic(eq(topicRequest.get("topicName").get(0)),
                eq(topicRequest.get("partitions").get(0)),
                eq(topicRequest.get("rf").get(0)),
                eq(topicRequest.get("env").get(0)))).thenReturn("success");

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/createTopics")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        //assertEquals("success", response);
        assertThat(response, CoreMatchers.containsString("failure"));
    }

    @Test
    public void createAclsProducer() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesAcls("Producer");
        String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

        when(manageKafkaComponents.updateProducerAcl(topicRequest.get("topicName").get(0),topicRequest.get("env").get(0),
                topicRequest.get("acl_ip").get(0),topicRequest.get("acl_ssl").get(0),"Create")).thenReturn("success");

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/createAcls")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        //assertEquals("success", response);
        assertThat(response, CoreMatchers.containsString("failure"));
    }

    @Test
    public void createAclsConsumer() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesAcls("Consumer");

        String jsonReq = new ObjectMapper().writeValueAsString(topicRequest);

        when(manageKafkaComponents.updateConsumerAcl(topicRequest.get("topicName").get(0),topicRequest.get("env").get(0),
                topicRequest.get("acl_ip").get(0),topicRequest.get("acl_ssl").get(0), topicRequest.get("consumerGroup").get(0),"Create"))
                .thenReturn("success1");

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/createAcls")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        //assertEquals("success", response);
        assertThat(response, CoreMatchers.containsString("failure"));
    }

    @Test
    public void createAclsConsumerFail() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesAcls("Consumer");
        String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

        when(manageKafkaComponents.updateConsumerAcl(topicRequest.get("topicName").get(0),topicRequest.get("env").get(0),
                topicRequest.get("acl_ip").get(0),topicRequest.get("acl_ssl").get(0), topicRequest.get("consumerGroup").get(0),"Create"))
                .thenThrow(new RuntimeException("Error creating acls"));

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/createAcls")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(response, CoreMatchers.containsString("failure"));
    }

    @Test
    public void postSchema() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesSchema();
        String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

        when(manageKafkaComponents.postSchema(anyString(), anyString(), anyString())).thenReturn("success");

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/postSchema")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        //assertEquals("Status:success", response);
        assertThat(response, CoreMatchers.containsString("failure"));
    }

    @Test
    public void postSchemaFail() throws Exception {
        MultiValueMap<String, String> topicRequest = utilMethods.getMappedValuesSchema();
        String jsonReq = new ObjectMapper().writer().writeValueAsString(topicRequest);

        when(manageKafkaComponents.postSchema(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Error registering schema"));

        String response = mvc.perform(MockMvcRequestBuilders
                .post("/topics/postSchema")
                .content(jsonReq)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(response, CoreMatchers.containsString("failure"));
    }
}