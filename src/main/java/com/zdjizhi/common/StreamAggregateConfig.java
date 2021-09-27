package com.zdjizhi.common;


import com.zdjizhi.utils.system.StreamAggregateConfigurations;

/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    public static final String FORMAT_SPLITTER = ",";
    public static final String PROTOCOL_SPLITTER = "\\.";

    /**
     * System
     */
    public static final Integer CONSUMER_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "consumer.parallelism");
    public static final Integer PARSE_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "parse.parallelism");
    public static final Integer APP_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0, "app.tick.tuple.freq.secs");
    public static final Integer COUNT_WINDOW_TIME = StreamAggregateConfigurations.getIntProperty(0, "count.window.time");
    public static final String TOOLS_LIBRARY = StreamAggregateConfigurations.getStringProperty(0, "tools.library");

    /**
     * kafka source
     */
    public static final String INPUT_KAFKA_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "input.kafka.servers");
    public static final String OUTPUT_KAFKA_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "output.kafka.servers");
    public static final String GROUP_ID = StreamAggregateConfigurations.getStringProperty(0, "group.id");
    public static final String OUTPUT_KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "output.kafka.topic");
    public static final String INPUT_KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "input.kafka.topic");
    public static final String PRODUCER_ACK = StreamAggregateConfigurations.getStringProperty(0, "producer.ack");
    public static final String KAFKA_SOURCE_PROTOCOL = StreamAggregateConfigurations.getStringProperty(1, "kafka.source.protocol");
    public static final String KAFKA_SINK_PROTOCOL = StreamAggregateConfigurations.getStringProperty(1, "kafka.sink.protocol");
    public static final String KAFKA_USER = StreamAggregateConfigurations.getStringProperty(1, "kafka.user");
    public static final String KAFKA_PIN = StreamAggregateConfigurations.getStringProperty(1, "kafka.pin");
    public static final String RETRIES = StreamAggregateConfigurations.getStringProperty(1, "retries");
    public static final String LINGER_MS = StreamAggregateConfigurations.getStringProperty(1, "linger.ms");
    public static final Integer REQUEST_TIMEOUT_MS = StreamAggregateConfigurations.getIntProperty(1, "request.timeout.ms");
    public static final Integer BATCH_SIZE = StreamAggregateConfigurations.getIntProperty(1, "batch.size");
    public static final Integer BUFFER_MEMORY = StreamAggregateConfigurations.getIntProperty(1, "buffer.memory");
    public static final Integer MAX_REQUEST_SIZE = StreamAggregateConfigurations.getIntProperty(1, "max.request.size");


    /**
     * kafka限流配置-20201117
     */
    public static final String PRODUCER_KAFKA_COMPRESSION_TYPE = StreamAggregateConfigurations.getStringProperty(0, "producer.kafka.compression.type");

    /**
     * http
     */
    public static final String SCHEMA_HTTP = StreamAggregateConfigurations.getStringProperty(0, "schema.http");
    public static final String APP_ID_HTTP = StreamAggregateConfigurations.getStringProperty(0, "app.id.http");


}