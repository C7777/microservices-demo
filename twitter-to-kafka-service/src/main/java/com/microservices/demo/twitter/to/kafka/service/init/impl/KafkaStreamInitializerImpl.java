package com.microservices.demo.twitter.to.kafka.service.init.impl;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.microservices.demo.twitter.to.kafka.service.init.StreamInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializerImpl implements StreamInitializer {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamInitializerImpl.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializerImpl(KafkaConfigData configData, KafkaAdminClient adminClient) {
        this.kafkaConfigData = configData;
        this.kafkaAdminClient = adminClient;
    }

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        log.info("Topics with name {} is ready for operations ", kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
