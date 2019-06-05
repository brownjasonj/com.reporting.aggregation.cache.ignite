package com.reporting.aggregation.cache.ignite;

import com.reporting.aggregation.cache.ignite.configuration.ApplicationConfig;
import com.reporting.aggregation.cache.ignite.kafka.RiskResultConsumerClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IgniteApplication {
	@Autowired
	ApplicationConfig applicationConfig;

	@Autowired
	RiskResultConsumerClient riskResultConsumerClient;

	@Autowired
	public IgniteApplication(RiskResultConsumerClient riskResultConsumerClient) {
		this.riskResultConsumerClient = riskResultConsumerClient;
		this.riskResultConsumerClient.run();
	}

	public static void main(String[] args) {
		SpringApplication.run(IgniteApplication.class, args);
	}

}
