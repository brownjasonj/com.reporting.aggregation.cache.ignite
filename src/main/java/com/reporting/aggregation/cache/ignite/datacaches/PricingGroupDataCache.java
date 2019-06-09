package com.reporting.aggregation.cache.ignite.datacaches;

import java.util.HashSet;

import com.reporting.mocks.model.id.MarketEnvId;

public class PricingGroupDataCache extends DataCache<String, HashSet<String>> {

    public PricingGroupDataCache(String cacheName) {
        super(cacheName);
    }
    
    public void addCacheName(MarketEnvId marketEnvId, String cacheName) {
        String marketIdName = marketEnvId.getId().toString();
        HashSet<String> cacheNames;
        if (this.cache.containsKey(marketIdName)) {
            cacheNames = this.cache.get(marketIdName);
        }
        else {
            cacheNames = new HashSet<>();
        }
        if (!cacheNames.contains(cacheName)) {
            cacheNames.add(cacheName);
            this.cache.put(marketIdName, cacheNames);
        }
    }
}