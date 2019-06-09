package com.reporting.aggregation.cache.ignite.datacaches;

import java.util.LinkedList;
import java.util.List;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;

public class DataCache<K,V> implements IDataCache<K,V> {
    protected String cacheName;
    protected List<IDataCacheListener<K,V>> listeners;
    protected Ignite ignite;
    protected IgniteCache<K,V> cache;
    protected ContinuousQuery<K,V> continuousQuery;

    public DataCache(String cacheName) {
        this.cacheName = cacheName;
        this.listeners = new LinkedList<>();
        this.ignite = IgniteCacheFactory.getIgnite();
        this.cache = ignite.getOrCreateCache(cacheName);
    }

    public void startCacheQuery() {
        // Creating a continuous query.
       this.continuousQuery = new ContinuousQuery<>();

       this.continuousQuery.setInitialQuery(new ScanQuery<>(null));

       // Callback that is called locally when update notifications are received.
       this.continuousQuery.setLocalListener(new CacheEntryUpdatedListener<K, V>() {
            @Override
            public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events)
                    throws CacheEntryListenerException {
                        for (CacheEntryEvent<? extends K, ? extends V> e : events) {
                            for(IDataCacheListener<K,V> listener : listeners) {
                                listener.update(e.getValue());
                            }
                        }
            }
       });

       // This filter will be evaluated remotely on all nodes.
       // Entry that pass this filter will be sent to the caller.
       this.continuousQuery.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<K, V>>() {
           private static final long serialVersionUID = 2067567893756427L;

            @Override
            public CacheEntryEventFilter<K, V> create() {
                return new CacheEntryEventFilter<K,V>() {
                    @Override
                    public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> event)
                            throws CacheEntryListenerException {
                        return true;
                    }
                };
            }
       });

       this.cache.query(this.continuousQuery);
    }

    @Override
    public List<V> getData() {
        List<V> results = new LinkedList<>();
        QueryCursor<Cache.Entry<K, V>> cursor = this.cache.query(new ScanQuery<>(null));
        for(Cache.Entry<K,V> entry : cursor) {
            results.add(entry.getValue());
        }
        return results;
    }

    @Override
    public List<V> getDataWindown(int from, int to) {
        List<V> results = new LinkedList<>();
        QueryCursor<Cache.Entry<K, V>> cursor = this.cache.query(new ScanQuery<>(null));
        for(Cache.Entry<K,V> entry : cursor) {
            results.add(entry.getValue());
        }
        return results;
    }

    @Override
    public String getName() {
        return this.cacheName;
    }

    @Override
    public void save(K key, V value) {
        this.cache.put(key, value);
    }

    @Override
    public Boolean containsKey(K key) {
        return this.cache.containsKey(key);
    }

    @Override
    public V getValue(K key) {
        return this.cache.get(key);
    }

    @Override
    public void addUpdateListener(IDataCacheListener<K, V> listener) {
        this.listeners.add(listener);
    }

    @Override
    public Boolean removeUpdateListener(IDataCacheListener<K, V> listener) {
        Boolean listenerExists = this.listeners.contains(listener);
        if (listenerExists) {
            this.listeners.remove(listener);
        }

        return listenerExists;
    }
}
