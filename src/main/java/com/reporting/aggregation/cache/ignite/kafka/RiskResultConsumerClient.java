package com.reporting.aggregation.cache.ignite.kafka;

import com.google.gson.Gson;
import com.reporting.aggregation.cache.ignite.configuration.ApplicationConfig;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;

import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.logging.Logger;

@Component
public class RiskResultConsumerClient implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(RiskResultConsumerClient.class.getName());

    private String BOOTSTRAPSERVER;
    private String RISKRESULT;
    private Properties kafkaProperties;
    private KafkaConsumer<UUID, String> consumer;

    protected Ignite ignite;

    @Autowired
    public RiskResultConsumerClient(ApplicationConfig applicationConfig) {
        this.BOOTSTRAPSERVER = applicationConfig.getKafkaServer();
        this.RISKRESULT = applicationConfig.getIntradayRiskSetTopic();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("group.id", "test1");
        this.kafkaProperties.put("key.deserializer", "com.reporting.kafka.serialization.UUIDDeserializer");
        this.kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<UUID, String>(this.kafkaProperties);
        this.consumer.subscribe(Collections.singletonList(this.RISKRESULT));
    }

    public void consume() {
        Gson gson = new Gson();
        try {
            while (true) {
                ConsumerRecords<UUID, String> records = this.consumer.poll(100);
                for (ConsumerRecord<UUID, String> record : records)
                {
                    RiskResult<? extends Risk> riskResult = gson.fromJson(record.value(), RiskResult.class);

                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void run() {
        System.out.println("RiskResult Kafka Consumer started");
        consume();
    }
}

