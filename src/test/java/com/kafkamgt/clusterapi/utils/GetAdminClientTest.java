package com.kafkamgt.clusterapi.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AdminClient.class)
public class GetAdminClientTest {

    @Mock
    Environment env;

    @Mock
    AdminClient adminClient;

    @Mock
    private ListTopicsResult listTopicsResult;

    @Mock
    private KafkaFuture<Set<String>> kafkaFuture;

    AdminClientUtils getAdminClient;

    @Before
    public void setUp() throws Exception {
       getAdminClient = new AdminClientUtils();
    }

    @Test
    public void getAdminClient1() throws ExecutionException, InterruptedException {
        ReflectionTestUtils.setField(getAdminClient, "env", env);
        mockStatic(AdminClient.class);

        when(env.getProperty(any())).thenReturn(null);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(kafkaFuture);
        Set<String> setStr = new HashSet<>();
        when(kafkaFuture.get()).thenReturn(setStr);
        BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);

        AdminClient result = getAdminClient.getAdminClient("localhost:9092");
        assertNotNull(result);
    }

    @Test
    public void getAdminClient2() throws ExecutionException, InterruptedException {
        ReflectionTestUtils.setField(getAdminClient, "env", env);
        mockStatic(AdminClient.class);

        when(env.getProperty(any())).thenReturn("true");
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(kafkaFuture);
        Set<String> setStr = new HashSet<>();
        when(kafkaFuture.get()).thenReturn(setStr);
        BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);

        AdminClient result = getAdminClient.getAdminClient("localhost:9092");
        assertNotNull(result);
    }

    @Test
    public void getAdminClient3() throws ExecutionException, InterruptedException {
        ReflectionTestUtils.setField(getAdminClient, "env", env);
        mockStatic(AdminClient.class);

        when(env.getProperty(any())).thenReturn("false");
        BDDMockito.given(AdminClient.create(any(Properties.class))).willReturn(adminClient);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(kafkaFuture);
        Set<String> setStr = new HashSet<>();
        when(kafkaFuture.get()).thenReturn(setStr);

        AdminClient result = getAdminClient.getAdminClient("localhost:9092");
        assertNotNull(result);
    }

    @Test
    public void getPlainProperties() {
        Properties props = getAdminClient.getPlainProperties("localhost");
        assertEquals("localhost", props.getProperty("bootstrap.servers"));
    }

    @Test
    public void getSslProperties() {
        ReflectionTestUtils.setField(getAdminClient, "env", env);
        when(env.getProperty(any())).thenReturn("somevalue");

        Properties props = getAdminClient.getSslProperties("localhost:9092");
        assertEquals("localhost:somevalue", props.getProperty("bootstrap.servers"));
    }
}