package com.kafkamgt.clusterapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;


@SpringBootApplication
public class KafkaclusterapiApplication {

    @Autowired
    Environment env;

    private static Logger LOG = LoggerFactory.getLogger(KafkaclusterapiApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaclusterapiApplication.class, args);
    }
}
