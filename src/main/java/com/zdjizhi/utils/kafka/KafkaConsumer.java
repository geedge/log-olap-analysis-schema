package com.zdjizhi.utils.kafka;

import com.zdjizhi.common.StreamAggregateConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.kafka
 * @Description:
 * @date 2021/6/813:54
 */
public class KafkaConsumer {
    private static Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", StreamAggregateConfig.SOURCE_KAFKA_SERVERS);
        properties.put("group.id", StreamAggregateConfig.GROUP_ID);
        properties.put("session.timeout.ms", StreamAggregateConfig.SESSION_TIMEOUT_MS);
        properties.put("max.poll.records", StreamAggregateConfig.MAX_POLL_RECORDS);
        properties.put("max.partition.fetch.bytes", StreamAggregateConfig.MAX_PARTITION_FETCH_BYTES);

        CertUtils.chooseCert(StreamAggregateConfig.SOURCE_KAFKA_SERVERS, properties);

        return properties;
    }

    /**
     * 官方序列化kafka数据
     *
     * @return kafka logs
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer() {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(StreamAggregateConfig.SOURCE_KAFKA_TOPIC,
                new SimpleStringSchema(), createConsumerConfig());

        //随着checkpoint提交，将offset提交到kafka
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        //从消费组当前的offset开始消费
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
}
