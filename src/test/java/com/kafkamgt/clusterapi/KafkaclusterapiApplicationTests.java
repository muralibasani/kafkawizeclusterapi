package com.kafkamgt.clusterapi;

import com.kafkamgt.clusterapi.controller.ClusterApiController;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest   (webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka
public class KafkaclusterapiApplicationTests {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbeddedBroker;

    @LocalServerPort
    private int port;

    @Test
    public void testGetTopics() {

        BrokerAddress brokerAddress = kafkaEmbeddedBroker.getBrokerAddresses()[0];
        String brokerAddressPort = brokerAddress.toString();
        String getTopicsEndpoint = "http://localhost:"+port+"/topics/getTopics/"+brokerAddressPort;

        Set<String> topics = this.restTemplate.getForObject(getTopicsEndpoint,Set.class);

        assertThat(topics.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testCreateTopic() {

        BrokerAddress brokerAddress = kafkaEmbeddedBroker.getBrokerAddresses()[0];
        String brokerAddressPort = brokerAddress.toString();
        String createTopicsEndpoint = "http://localhost:"+port+"/topics/createTopics/";

        MultiValueMap<String, String> params= new LinkedMultiValueMap<String, String>();

        params.add("env",brokerAddressPort);
        params.add("topicName","testtopic6");
        params.add("acl_ip","127.0.0.1");
        params.add("acl_ssl",null);
        params.add("rf", "1");
        params.add("partitions", "2");

        String topicsCreateResponse = this.restTemplate.postForObject(createTopicsEndpoint, params, String.class);

        assertThat(topicsCreateResponse).contains("success");

        String getTopicsEndpoint = "http://localhost:"+port+"/topics/getTopics/"+brokerAddressPort;

        Set<String> topics = this.restTemplate.getForObject(getTopicsEndpoint,Set.class);

        long topicCreateCount = topics.stream().filter(a->{
                if(a.equals("testtopic6:::::1:::::2"))  // rf 1, partitions 2
                    return true;
                else
                    return false;
                })
                .count();

        assertThat(topicCreateCount).isEqualTo(1);
    }

}
