package com.reporting.aggregation.cache.ignite.kafka;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.reporting.aggregation.cache.ignite.configuration.ApplicationConfig;
import com.reporting.aggregation.cache.ignite.datacaches.IgniteCacheFactory;
import com.reporting.aggregation.cache.ignite.datacaches.PricingGroupDataCache;
import com.reporting.aggregation.cache.ignite.datacaches.RiskResultDataCache;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;

import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
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

    protected HashMap<String, RiskResultDataCache> riskResultDataCaches;
    protected HashMap<String, PricingGroupDataCache> pricingGroupDataCaches;
    
    @Autowired
    public RiskResultConsumerClient(ApplicationConfig applicationConfig) {
        this.BOOTSTRAPSERVER = applicationConfig.getKafkaServer();
        this.RISKRESULT = applicationConfig.getIntradayRiskTickTopic();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("group.id", "test1");
        this.kafkaProperties.put("key.deserializer", "com.reporting.kafka.serialization.UUIDDeserializer");
        this.kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<UUID, String>(this.kafkaProperties);
        this.consumer.subscribe(Collections.singletonList(this.RISKRESULT));

        this.ignite = IgniteCacheFactory.getIgnite();

        this.riskResultDataCaches = new HashMap<>();
        this.pricingGroupDataCaches = new HashMap<>();
    }

    public void consume() {
        Gson gson = new Gson();
        try {
            while (true) {
                ConsumerRecords<UUID, String> records = this.consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<UUID, String> record : records)
                {
                    RiskResult<? extends Risk> riskResult = gson.fromJson(record.value(), new TypeToken<RiskResult<? extends Risk>>(){}.getType());

                    String pricingGroup = riskResult.getMarketEnvId().getPricingGroupName();

                    PricingGroupDataCache pgDataCache;
                    if (this.pricingGroupDataCaches.containsKey(pricingGroup)) {
                        pgDataCache = this.pricingGroupDataCaches.get(pricingGroup);
                    }
                    else {
                        pgDataCache = new PricingGroupDataCache(pricingGroup);
                        this.pricingGroupDataCaches.put(pricingGroup, pgDataCache);
                    }

                    // create the name of the  cache for this particular riskResult  The cache name has the form
                    //
                    //  <marketid>/<book>/<risktype>
                    //
                    String marketIdName = riskResult.getMarketEnvId().getId().toString();
                    String riskResultPathName = riskResult.getRisk().getBookName() + "/" + riskResult.getRisk().getRiskType();
                    String riskResultCacheName = marketIdName + "/" + riskResultPathName;

                    pgDataCache.addCacheName(riskResult.getMarketEnvId(), riskResultCacheName);

                    RiskResultDataCache rrDataCache;
                    if (riskResultDataCaches.containsKey(riskResultCacheName)) {
                        rrDataCache = riskResultDataCaches.get(riskResultCacheName);
                    }
                    else {
                        rrDataCache = new RiskResultDataCache(riskResultCacheName);
                        riskResultDataCaches.put(riskResultCacheName, rrDataCache);
                    }

                    LOGGER.info("Writing RiskResult to cache " + riskResultCacheName);
                    rrDataCache.save(riskResult.getRisk().getTcn().getId(), riskResult);
                }
            }
        } finally {
            consumer.close();
        }
    }
/*
    public void consume() {
        Gson gson = new Gson();
        try {
            while (true) {
                ConsumerRecords<UUID, String> records = this.consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<UUID, String> record : records)
                {
                    RiskResult<? extends Risk> riskResult = gson.fromJson(record.value(), new TypeToken<RiskResult<? extends Risk>>(){}.getType());

                    // get (or create if non-existent) a cache to hold the list of caches for a given pricing group
                    // this cache can be used by any consumer that wants to know what caches are available for a particular pricinggroup
                    IgniteCache<String, HashSet<String>> marketCache = ignite.getOrCreateCache(riskResult.getMarketEnvId().getPricingGroupName());

                    // create the name of the  cache for this particular riskResult
                    String marketIdName = riskResult.getMarketEnvId().getId().toString();
                    String riskResultPathName = riskResult.getRisk().getBookName() + "/" + riskResult.getRisk().getRiskType();

                    HashSet<String> cacheNames;
                    if (marketCache.containsKey(marketIdName)) {
                        cacheNames = marketCache.get(marketIdName);
                    }
                    else {
                        cacheNames = new HashSet<>();
                    }
                    if (!cacheNames.contains(riskResultPathName)) {
                        cacheNames.add(riskResultPathName);
                        marketCache.put(marketIdName, cacheNames);
                    }

                    IgniteCache<UUID, RiskResult<? extends Risk>> riskCache = ignite.getOrCreateCache(marketIdName + "/" + riskResultPathName);
                    
                    riskCache.put(riskResult.getRisk().getTcn().getId(), riskResult);
                }
            }
        } finally {
            consumer.close();
        }
    }
    */

    @Override
    public void run() {
        LOGGER.info("RiskResult Kafka Consumer started");
        consume();
    }
}

