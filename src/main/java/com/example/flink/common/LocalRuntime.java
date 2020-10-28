package com.example.flink.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LocalRuntime {
    public static Map<String, Properties> getApplicationProperties(String[] args) throws IOException {
        Properties rdsConfig = new Properties();
        Properties rabbitConsumerConfig = new Properties();
        Properties rabbitSinkConfig = new Properties();
        Properties appConfig = new Properties();

        ParameterTool parameter = ParameterTool.fromArgs(args);

        appConfig.setProperty("minutes", parameter.get("minutes", "1"));
        appConfig.setProperty("mode.debug", parameter.get("mode.debug", "true"));
        appConfig.put("issinktoqueue", parameter.get("issinktoqueue", "true"));

        appConfig.setProperty("start.am", parameter.get("start.am","8:45"));
        appConfig.setProperty("end.am", parameter.get("end.am","11:30"));
        appConfig.setProperty("start.pm", parameter.get("start.pm","13:00"));
        appConfig.setProperty("end.pm", parameter.get("end.pm","21:15"));

        rdsConfig.put("username", parameter.get("rds.name"));
        rdsConfig.put("password", parameter.get("rds.password"));
        rdsConfig.put("drivername", parameter.get("rds.drivername"));
        rdsConfig.put("url", parameter.get("rds.url"));
        rdsConfig.put("table.name", parameter.get("rds.table"));

        rabbitConsumerConfig.put("host", parameter.get("rb.host"));
        rabbitConsumerConfig.put("port", parameter.get("rb.port"));
        rabbitConsumerConfig.put("username", parameter.get("rb.username"));
        rabbitConsumerConfig.put("password", parameter.get("rb.password"));
        rabbitConsumerConfig.put("vhost", parameter.get("rb.vhost"));
        rabbitConsumerConfig.put("queue.name", parameter.get("rb.queue.name"));

        rabbitSinkConfig.put("host", parameter.get("rbsink.host"));
        rabbitSinkConfig.put("port", parameter.get("rbsink.port"));
        rabbitSinkConfig.put("username", parameter.get("rbsink.username"));
        rabbitSinkConfig.put("password", parameter.get("rbsink.password"));
        rabbitSinkConfig.put("vhost", parameter.get("rbsink.vhost"));
        rabbitSinkConfig.put("exchange", parameter.get("rbsink.exchange"));
        rabbitSinkConfig.put("rkprefix", parameter.get("rbsink.rkprefix"));

        Map<String, Properties> rs = new HashMap<>();
        rs.put("AppConfigProperties", appConfig);
        rs.put("RabbitConsumer", rabbitConsumerConfig);
        rs.put("RabbitSink", rabbitSinkConfig);
        rs.put("RdsConfigProperties", rdsConfig);

        System.out.println(appConfig);
        System.out.println(rabbitConsumerConfig);
        System.out.println(rabbitSinkConfig);
        System.out.println(rdsConfig);

        return rs;
    }
}
