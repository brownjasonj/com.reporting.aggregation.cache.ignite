package com.reporting.aggregation.cache.ignite.datacaches;

public interface IDataCacheListener<K,V> {
    void update(V updatedValue);
}