package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunnerImpl implements StreamRunner {

    private static final Logger log = LoggerFactory.getLogger(MockKafkaStreamRunnerImpl.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();


    private static final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyy";

    public MockKafkaStreamRunnerImpl(TwitterToKafkaServiceConfigData configData,
                                 TwitterKafkaStatusListener statusListener) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterKafkaStatusListener = statusListener;
    }

    @Override
    public void start() throws TwitterException {

        String[] keyWords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        Integer mockMinTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        Integer mockMaxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        Long mockSleepMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        log.info("Starting mock twitter stream");
        simulateTwitterStream(keyWords, mockMinTweetLength, mockMaxTweetLength, mockSleepMs);
    }

    private void simulateTwitterStream(String[] keyWords, Integer mockMinTweetLength, Integer mockMaxTweetLength,
                                       Long mockSleepMs) {

        Executors.newSingleThreadExecutor().submit(() -> {
            try{
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keyWords, mockMinTweetLength, mockMaxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(mockSleepMs);
                }
            }catch (TwitterException e) {
                log.error("Error creating twitter status", e);
            }
        });

    }

    private void sleep(Long mockSleepMs){
        try {
            Thread.sleep(mockSleepMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create");
        }
    }

    private String getFormattedTweet(String[] keyWords, Integer mockMinTweetLength, Integer mockMaxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keyWords, mockMinTweetLength, mockMaxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };

        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keyWords, Integer mockMinTweetLength, Integer mockMaxTweetLength) {

        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(mockMaxTweetLength - mockMinTweetLength + 1) + mockMinTweetLength;
        return constructRandomTweet(keyWords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keyWords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2){
                tweet.append(keyWords[RANDOM.nextInt(keyWords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
