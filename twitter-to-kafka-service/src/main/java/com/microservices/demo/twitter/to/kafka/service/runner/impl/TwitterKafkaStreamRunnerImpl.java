package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunnerImpl implements StreamRunner {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaStreamRunnerImpl.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunnerImpl(TwitterToKafkaServiceConfigData configData,
                                    TwitterKafkaStatusListener statusListener) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterKafkaStatusListener = statusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    private void addFilter() {
        String[] twitterKeywordsArray = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(twitterKeywordsArray);
        twitterStream.filter(filterQuery);
        log.info("Started filtering of keywords");
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream != null){
            log.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }
}
