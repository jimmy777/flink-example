package com.yzy.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 16:28
 * @Description socket -> kafka sink 例子。
 *
 * 注意：
 * 1、Flink 处理的数据结果写入 kafka；
 * 2、FlinkKafkaProducer 对象负责往 kafka 写入数据。
 * 3、Kafka Sink 写入数据是并行处理的
 *
 */
public class KafkaSink {

    public static void main(String[] args) throws Exception {

        //-------------------- env --------------------//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //-------------------- source --------------------//
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666);


        //-------------------- sink --------------------//
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.209.101:9092");
        properties.setProperty("retries", "3");
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("test", new ProducerStringSerializationSchema("test"), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


        // 较老的 API 调用
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("test", new SimpleStringSchema(), properties);
        // 带分区的 API 调用
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("test", new SimpleStringSchema(), properties);


        socket.addSink(kafkaProducer);

        // env.fromCollection(list).addSink(kafkaProducer).setParallelism(4);


        //-------------------- submit --------------------//
        env.execute("my job.");
    }
}
