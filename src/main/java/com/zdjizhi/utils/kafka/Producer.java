package com.zdjizhi.utils.kafka;

import com.zdjizhi.common.StreamAggregateConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Optional;
import java.util.Properties;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.kafka
 * @Description:
 * @date 2021/6/814:04
 */
public class Producer {

    private static Properties createProducerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", StreamAggregateConfig.SINK_KAFKA_SERVERS);
        properties.put("acks", StreamAggregateConfig.PRODUCER_ACK);
        properties.put("retries", StreamAggregateConfig.RETRIES);
        properties.put("linger.ms", StreamAggregateConfig.LINGER_MS);
        properties.put("request.timeout.ms", StreamAggregateConfig.REQUEST_TIMEOUT_MS);
        properties.put("batch.size", StreamAggregateConfig.BATCH_SIZE);
        properties.put("buffer.memory", StreamAggregateConfig.BUFFER_MEMORY);
        properties.put("max.request.size", StreamAggregateConfig.MAX_REQUEST_SIZE);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, StreamAggregateConfig.PRODUCER_KAFKA_COMPRESSION_TYPE);

        CertUtils.chooseCert(StreamAggregateConfig.KAFKA_SINK_PROTOCOL, properties);

        return properties;
    }


    public static FlinkKafkaProducer<String> getKafkaProducer() {
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                StreamAggregateConfig.SINK_KAFKA_TOPIC,
                new SimpleStringSchema(),
                createProducerConfig(), Optional.empty());

        //启用此选项将使生产者仅记录失败日志而不是捕获和重新抛出它们
        kafkaProducer.setLogFailuresOnly(false);

        //写入kafka的消息携带时间戳
//        kafkaProducer.setWriteTimestampToKafka(true);


        return kafkaProducer;
    }
}
