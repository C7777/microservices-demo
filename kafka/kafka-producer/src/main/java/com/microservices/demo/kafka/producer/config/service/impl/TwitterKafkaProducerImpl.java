package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducerImpl implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaProducerImpl.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducerImpl(KafkaTemplate<Long, TwitterAvroModel> template) {
        this.kafkaTemplate = template;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        log.info("Sending message={} to topic={}", message, topicName);
        log.info("Bootstrap servers information: {}",
                kafkaTemplate.getProducerFactory().getConfigurationProperties().
                        get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        ListenableFuture<SendResult<Long, TwitterAvroModel>> sendResultListenableFuture =
                kafkaTemplate.send(topicName, key, message);
        addCallback(topicName, message, sendResultListenableFuture);
    }

    private void addCallback(String topicName, TwitterAvroModel message,
                             ListenableFuture<SendResult<Long, TwitterAvroModel>> sendResultListenableFuture) {
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error while sending message {} to topic {}", message.toString(), topicName, ex);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                log.debug("Received new metadata. Topic {}; Partition {}; Offset {}; Timestamp {}; at time {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                        recordMetadata.timestamp(), System.nanoTime());
            }
        });
    }

    @PreDestroy
    public void close(){
        if(kafkaTemplate != null){
            log.info("Closing kafka producer");
            kafkaTemplate.destroy();
        }
    }
}
