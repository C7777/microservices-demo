package com.microservices.demo.common.config;

import com.microservices.demo.config.RetryConfigData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    private RetryConfigData retryConfigData;

    public RetryConfig(RetryConfigData configData) {
        this.retryConfigData = configData;
    }


    @Bean
    public RetryTemplate retryTemplate(){
        RetryTemplate retryTemplate = new RetryTemplate();
        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(retryConfigData.getInitialIntervalMs());
        exponentialBackOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
        System.out.println("Started setting");
        exponentialBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());
        System.out.println("Multiplier setting done");
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(retryConfigData.getMaxAttempts());
        retryTemplate.setRetryPolicy(simpleRetryPolicy);
        System.out.println("Setting done");
        return retryTemplate;
    }


}
