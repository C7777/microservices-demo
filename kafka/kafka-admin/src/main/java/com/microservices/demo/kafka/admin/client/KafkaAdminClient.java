package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;


    public KafkaAdminClient(KafkaConfigData config,
                            RetryConfigData retryConfigData,
                            AdminClient client,
                            RetryTemplate template,
                            WebClient webClient) {
        this.kafkaConfigData = config;
        this.retryConfigData = retryConfigData;
        this.adminClient = client;
        this.retryTemplate = template;
        this.webClient = webClient;
    }

    public void createTopics(){
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new RuntimeException("Reached max number of retry for creating kafka topics", t);
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNamesToCreate = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating {} topics, attempt {}", topicNamesToCreate.size(), retryContext.getRetryCount());
        List<NewTopic> newTopicList = topicNamesToCreate.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(newTopicList);
    }

    public void checkTopicsCreated(){
        Collection<TopicListing> topicListings = getTopics();
        int retryCount = 1;
        Integer maxAttempts = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topicListings, topic)){
                checkMaxRetry(retryCount++, maxAttempts);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topicListings = getTopics();
            }
        }
    }

    public void checkSchemaRegistry(){
        int retryCount = 1;
        Integer maxAttempts = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()){
            checkMaxRetry(retryCount++, maxAttempts);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus(){
        try {
            return webClient.method(HttpMethod.GET).uri(kafkaConfigData.getSchemaRegistryUrl()).exchange()
                    .map(ClientResponse::statusCode).block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping for waiting new Created Topics");
        }
    }

    private void checkMaxRetry(int retryCount, Integer maxAttempts) {
        if(retryCount > maxAttempts){
            throw new KafkaClientException("Reached Max number of retry for reading Kafka Topics");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topicListings, String topic) {
        if(topicListings == null){
            return false;
        }
        return topicListings.stream().anyMatch(topicL -> topic.equals(topicL.name()));
    }

    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topicListings;
        try {
            topicListings = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for reading kafka topics", t);
        }
        return topicListings;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext)
            throws ExecutionException, InterruptedException {
        log.info("Reading Kafka topic {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(),
                retryContext.getRetryCount());
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();
        if(topicListings != null){
            topicListings.forEach(topic -> log.debug("Topic with name {}",topic.name()));
        }
        return topicListings;
    }
}
