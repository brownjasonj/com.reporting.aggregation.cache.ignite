package com.reporting.aggregation.cache.ignite.datacaches;

import java.util.List;

public interface IDataCache<K, V> {
    String getName();

    void save(K key, V value);
    Boolean containsKey(K key);

    List<V> getData();
    List<V> getDataWindown(int from, int to);
    V getValue(K key);

    void addUpdateListener(IDataCacheListener<K,V> listener);
    Boolean removeUpdateListener(IDataCacheListener<K,V> listener);

}