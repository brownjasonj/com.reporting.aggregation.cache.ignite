package com.reporting.aggregation.cache.ignite.datacaches;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class IgniteCacheFactory {
    protected static Ignite ignite = null;

    public static void connect() {
        if (ignite == null) {
            Ignition.setClientMode(true);
            IgniteCacheFactory.ignite = Ignition.start("examples/config/example-ignite.xml");
        }
    }

    public static Ignite getIgnite() {
        connect();
        return IgniteCacheFactory.ignite;
    }
}
