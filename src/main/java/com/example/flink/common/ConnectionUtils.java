package com.example.flink.common;

import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Properties;

public class ConnectionUtils {
    public static RMQConnectionConfig getRMQCnnConfig(Properties properties) {
        return new RMQConnectionConfig.Builder()
                .setHost(properties.getProperty("host"))
                .setPort(Integer.parseInt(properties.getProperty("port")))
                .setUserName(properties.getProperty("username"))
                .setPassword(properties.getProperty("password"))
                .setVirtualHost(properties.getProperty("vhost"))
                .setAutomaticRecovery(true)
                .setTopologyRecoveryEnabled(true)
                .setConnectionTimeout(2000) // 2000ms
                .build();
    }
}
