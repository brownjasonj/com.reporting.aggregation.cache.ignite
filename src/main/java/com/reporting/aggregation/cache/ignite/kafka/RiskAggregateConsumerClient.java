package com.reporting.aggregation.cache.ignite.kafka;

import com.google.gson.Gson;
import com.reporting.aggregation.cache.ignite.configuration.ApplicationConfig;
import com.reporting.mocks.model.risks.Pv;
import com.reporting.mocks.model.risks.aggregate.DeltaAggregate;
import com.reporting.mocks.model.risks.aggregate.PvAggregate;
import com.reporting.mocks.model.risks.aggregate.RiskAggregate;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.logging.Logger;

@Component
public class RiskAggregateConsumerClient implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(RiskAggregateConsumerClient.class.getName());
    
    private String BOOTSTRAPSERVER;
    private String RISKRESULT;
    private Properties kafkaProperties;
    private KafkaConsumer<UUID, String> consumer;


    protected Ignite ignite;

    @Autowired
    public RiskAggregateConsumerClient(ApplicationConfig applicationConfig) {
        this.BOOTSTRAPSERVER = applicationConfig.getKafkaServer();
        this.RISKRESULT = applicationConfig.getRiskAggregatesTopic();

        Ignition.setClientMode(true);
        this.ignite = Ignition.start("examples/config/example-ignite.xml");

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
                    RiskAggregate riskAggregate = gson.fromJson(record.value(), RiskAggregate.class);

                    switch (riskAggregate.getRiskType()) {
                        case PV:
                            PvAggregate pvAggregate = gson.fromJson(record.value(), PvAggregate.class);
                            IgniteCache<String, PvAggregate> pvCache = ignite.getOrCreateCache(riskAggregate.getMarketEnvId().getId().toString());
                            pvCache.put(pvAggregate.getBookName(), pvAggregate);
                            break;
                        case DELTA:
                            DeltaAggregate deltaAggregate = gson.fromJson(record.value(), DeltaAggregate.class);
                            IgniteCache<String, DeltaAggregate> cache = ignite.getOrCreateCache(riskAggregate.getMarketEnvId().getId().toString());
                            cache.put(deltaAggregate.getBookName(), deltaAggregate);
                            break;
                        case GAMMA:
                        case VEGA:
                        default:
                            break;
                    }

                }
            }
        } finally {
            consumer.close();
        }
    }
    @Override
    public void run() {
        System.out.println("RiskAggregate Kafka/Ignite Consumer/Publisher started");
        consume();
    }
}
