package com.yzy.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/23 9:50
 * @Description 内置source，如：Kafka source源-> Flink处理。
 *
 * 1. ParallelSourceFunction 和 RichParallelSourceFunction 为并行方法。如：Kafka 源。
 * 2. 基于 Kafka 的 partition 分区机制，Flink 实现了并行化数据切分；
 * 3. Flink 可以消费 Kafka 的 topic， 或者 sink 数据到 Kafka；
 * 4. 出现失败时，Flink 通过 checkpoint 机制来协调 Kafka 来恢复应用。（通过设置 Kafka 的 offset）
 *
 */
public class KafkaRichParallelSourceMain {

    public static void main(String[] args) throws Exception {

        // 创建本地环境env并开启webui。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 配置一些 kafka 服务的属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");
        // 每30秒检查发现kafka的topic分区是否有变化，这样Flink自适应发现分区变化。
        properties.setProperty("flink.partition-discovery.interval-millis", "30000");

        // 方法一：直接使用内置的SimpleStringSchema字符串解析类来处理String类型。
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        // 方法二：使用自定义的类MyKafkaRecordSchema解析成java对象。
        // FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // sink输出为print打印
        env.addSource(kafkaSource).print();

        // 提交任务并执行。
        env.execute("my kafka job.");
    }
}
