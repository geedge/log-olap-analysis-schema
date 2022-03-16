package com.zdjizhi.common;


import com.zdjizhi.utils.system.StreamAggregateConfigurations;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    private static StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();

    static {
        encryptor.setPassword("galaxy");
    }

    public static final String FORMAT_SPLITTER = ",";
    public static final String PROTOCOL_SPLITTER = "\\.";

    /**
     * System
     */
    public static final Integer SOURCE_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "source.parallelism");
    public static final Integer PARSE_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "parse.parallelism");
    public static final Integer FIRST_WINDOW_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "first.window.parallelism");
    public static final Integer SECOND_WINDOW_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "second.window.parallelism");
    public static final Integer APP_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(1, "app.tick.tuple.freq.secs");
    public static final Integer FIRST_COUNT_WINDOW_TIME = StreamAggregateConfigurations.getIntProperty(0, "first.count.window.time");
    public static final Integer SECOND_COUNT_WINDOW_TIME = StreamAggregateConfigurations.getIntProperty(0, "second.count.window.time");
    public static final String TOOLS_LIBRARY = StreamAggregateConfigurations.getStringProperty(0, "tools.library");
    public static final Integer BUFFER_TIMEOUT = StreamAggregateConfigurations.getIntProperty(1, "buffer.timeout");
    public static final Integer SINK_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "sink.parallelism");
    public static final Integer RANDOM_RANGE_NUM = StreamAggregateConfigurations.getIntProperty(1, "random.range.num");

    /**
     * Kafka common
     */
    public static final String KAFKA_SASL_JAAS_USER = encryptor.decrypt(StreamAggregateConfigurations.getStringProperty(1, "kafka.user"));
    public static final String KAFKA_SASL_JAAS_PIN = encryptor.decrypt(StreamAggregateConfigurations.getStringProperty(1, "kafka.pin"));


    /**
     * kafka sink config
     */
    public static final String SINK_KAFKA_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "sink.kafka.servers");
    public static final String SINK_KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "sink.kafka.topic");
    public static final String PRODUCER_ACK = StreamAggregateConfigurations.getStringProperty(0, "producer.ack");
    public static final String RETRIES = StreamAggregateConfigurations.getStringProperty(1, "retries");
    public static final String LINGER_MS = StreamAggregateConfigurations.getStringProperty(1, "linger.ms");
    public static final Integer REQUEST_TIMEOUT_MS = StreamAggregateConfigurations.getIntProperty(1, "request.timeout.ms");
    public static final Integer BATCH_SIZE = StreamAggregateConfigurations.getIntProperty(1, "batch.size");
    public static final Integer BUFFER_MEMORY = StreamAggregateConfigurations.getIntProperty(1, "buffer.memory");
    public static final Integer MAX_REQUEST_SIZE = StreamAggregateConfigurations.getIntProperty(1, "max.request.size");


    /**
     * kafka source config
     */
    public static final String SOURCE_KAFKA_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "source.kafka.servers");
    public static final String SOURCE_KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "source.kafka.topic");
    public static final String GROUP_ID = StreamAggregateConfigurations.getStringProperty(0, "group.id");
    public static final String SESSION_TIMEOUT_MS = StreamAggregateConfigurations.getStringProperty(1, "session.timeout.ms");
    public static final String MAX_POLL_RECORDS = StreamAggregateConfigurations.getStringProperty(1, "max.poll.records");
    public static final String MAX_PARTITION_FETCH_BYTES = StreamAggregateConfigurations.getStringProperty(1, "max.partition.fetch.bytes");


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