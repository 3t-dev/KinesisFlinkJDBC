package com.example.flink.customer;

import com.example.flink.proto.TransLogProtos;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DurableRMQSource<OUT> extends RMQSource<OUT> {

    public static Logger logger = LoggerFactory.getLogger(DurableRMQSource.class);

    private final RMQConnectionConfig cnnCfg;
    private final String qName;

    public DurableRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<OUT> deserializationSchema) {
        super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);

        this.cnnCfg = rmqConnectionConfig;
        this.qName = queueName;
    }

    @Override
    protected void setupQueue() throws IOException {
        channel.queueDeclare(this.qName, true, false, false, null);
    }

    @Override
    protected void acknowledgeSessionIDs(List<Long> sessionIds) {
        if(!this.autoAck) {
            logger.info("Rabbit source is not auto ack, so have to manually ack");
            super.acknowledgeSessionIDs(sessionIds);
        } else {
            logger.info("Rabbit source is auto ack");
        }
    }
}
