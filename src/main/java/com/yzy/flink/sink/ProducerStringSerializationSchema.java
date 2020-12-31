package com.yzy.flink.sink;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/30 10:35
 * @Description
 */
public class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {

    private String topic;

    public ProducerStringSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
    }
}
