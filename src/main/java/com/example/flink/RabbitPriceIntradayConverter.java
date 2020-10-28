package com.example.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.rabbitmq.client.AMQP;
import com.example.flink.common.*;
import com.example.flink.customer.PriceIntradaySink;
import com.example.flink.customer.DurableRMQSource;
import com.example.flink.model.PriceIntradayBar;
import com.example.flink.proto.TransLogProtos.TransLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class RabbitPriceIntradayConverter {
    public static Logger logger = LoggerFactory.getLogger(RabbitPriceIntradayConverter.class);
    private static final double ESP_DOUBLE = 0.001;

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        boolean devEnv = parameter.getBoolean("env.dev", false);
        Map<String, Properties> applicationProperties = devEnv ? LocalRuntime.getApplicationProperties(args) : KinesisAnalyticsRuntime.getApplicationProperties();

        final Properties rabbitConsumerConfig = applicationProperties.get("RabbitConsumer");
        final Properties rdsConfig = applicationProperties.get("RdsConfigProperties");
        final Properties appConfig = applicationProperties.get("AppConfigProperties");
        
        int minutes = Integer.parseInt(appConfig.getProperty("minutes"));
        boolean debugMode = Boolean.parseBoolean(appConfig.getProperty("mode.debug", "false"));
        String queueName = rabbitConsumerConfig.getProperty("queue.name");

        // Checking input parameters
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitConsumerConfig.getProperty("host"))
                .setPort(Integer.parseInt(rabbitConsumerConfig.getProperty("port")))
                .setUserName(rabbitConsumerConfig.getProperty("username"))
                .setPassword(rabbitConsumerConfig.getProperty("password"))
                .setVirtualHost(rabbitConsumerConfig.getProperty("vhost"))
                .build();

        DataStream<TransLog> transLogDS = env.addSource(new DurableRMQSource<TransLog>(
                connectionConfig,
                queueName,
                false,
                new DeserializationSchema<TransLog>() {
                    @Override
                    public boolean isEndOfStream(TransLog transLog) {
                        return false;
                    }

                    @Override
                    public TransLog deserialize(byte[] val) {
                        try {
                            return TransLog.parseFrom(val);
                        } catch (Exception ex1) {
                            logger.warn("not support protobuf type");
                        }
                        return null;
                    }

                    @Override
                    public TypeInformation<TransLog> getProducedType() {
                        return TypeInformation.of(TransLog.class);
                    }
                }
        )).setParallelism(1).name("Source intraday " + minutes);

        final DataStream<PriceIntradayBar> intraBarDS = transLogDS
                .filter(new FilterTransLog())
                .map(new MapFunction<TransLog, Tuple2<String, TransLog>>() {
                    @Override
                    public Tuple2<String, TransLog> map(TransLog transLog) throws Exception {
                        return new Tuple2<String, TransLog>(transLog.getSymbol(), transLog);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(minutes))
                .apply(new PriceIntradayBar(),
                        new FoldFunction<Tuple2<String, TransLog>, PriceIntradayBar>() {
                            @Override
                            public PriceIntradayBar fold(PriceIntradayBar priceIntradayBar, Tuple2<String, TransLog> o) throws Exception {
                                priceIntradayBar.setSymbol(o.f0);
                                double matchPrice = Double.parseDouble(o.f1.getFormattedMatchPrice());
                                double cumulative = Double.parseDouble(o.f1.getFormattedAccVol());
                                int matchVol = Long.valueOf(o.f1.getFormattedVol()).intValue();

                                if (priceIntradayBar.getOpen() < ESP_DOUBLE) {
                                    priceIntradayBar.setOpen(matchPrice);
                                    priceIntradayBar.setChangeVal(Double.parseDouble(o.f1.getFormattedChangeValue()));
                                }
                                priceIntradayBar.setClose(matchPrice);
                                if (priceIntradayBar.getHigh() < ESP_DOUBLE || priceIntradayBar.getHigh() < matchPrice)
                                    priceIntradayBar.setHigh(matchPrice);
                                if (priceIntradayBar.getLow() < ESP_DOUBLE || priceIntradayBar.getLow() > matchPrice)
                                    priceIntradayBar.setLow(matchPrice);
                                if (priceIntradayBar.getCumulativeVol() < ESP_DOUBLE || priceIntradayBar.getCumulativeVol() < cumulative)
                                    priceIntradayBar.setCumulativeVol(cumulative);

                                priceIntradayBar.setVolume(priceIntradayBar.getVolume() + matchVol);

                                if (debugMode) {
                                    if(devEnv) {
                                        System.out.println("PriceIntradayBar fold: " + priceIntradayBar.getSymbol() + ", " + matchPrice + ", " + new Date().toString());
                                    } else {
                                        logger.info("PriceIntradayBar fold: " + priceIntradayBar.getSymbol() + ", " + matchPrice + ", " + new Date().toString());
                                    }
                                }
                                return priceIntradayBar;
                            }
                        },
                        new WindowFunction<PriceIntradayBar, PriceIntradayBar, Tuple, TimeWindow>() {
                            @Override
                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<PriceIntradayBar> iterable,
                                    Collector<PriceIntradayBar> collector) throws Exception {
                                Timestamp ts = new Timestamp(timeWindow.getStart());
                                Date date = new Date(ts.getTime());
                                Instant instant = date.toInstant();
                                String ticker = "";
                                for (PriceIntradayBar price : iterable) {
                                    price.setTimeSec(instant.getEpochSecond());
                                    ticker = price.getSymbol();
                                    collector.collect(price);
                                }
                                if (debugMode) {
                                    if(devEnv) {
                                        System.out.println("PriceIntradayBar apply: " + ticker + ", " + date.toString());
                                    } else {
                                        logger.info("PriceIntradayBar apply: " + ticker + ", " + date.toString());
                                    }
                                }
                            }
                        }
                )
                .name("Calculate instraday " + minutes);
        
        intraBarDS.addSink(new PriceIntradaySink(rdsConfig))
                .name("store RDS " + minutes + " minute");

        env.execute("Price Intraday Converter " + minutes);
    }

    public static final class FilterTransLog implements FilterFunction<TransLog> {
        @Override
        public boolean filter(TransLog s) throws Exception {
            return s.getSymbol() != null && !s.getSymbol().isEmpty();
        }
    }
}