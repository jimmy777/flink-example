package com.yzy.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 11:19
 * @Description kafka connector.
 *
 * 过程 3 步：
 * 1. 配置 kafka 参数；
 * 2. 创建 FlinkKafkaConsumer 对象；
 * 3. kafka 加入到 flink source 中，生产 DataStream 对象。
 *
 * 注意：
 *   kafka 也可以作为 sink，将结果数据写入 topic 中。
 */

public class KafkaConnector {

    public static void main(String[] args) throws Exception {

        //--- env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(3);

        //--- kafka source
        // 配置 kafka 属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");
        // 每30秒检查发现kafka的topic分区是否有变化，这样Flink自适应发现分区变化。
        // properties.setProperty("flink.partition-discovery.interval-millis", "30000");
        /*
        If you get data in a different format, then you can also specify your custom schema for deserialization. By default, Flink supports string and JSON deserializers.
        这里直接使用内置的 SimpleStringSchema 字符串解析类来处理 String 类型。直接使用内置的 SimpleStringSchema 字符串解析类来处理 String 类型。
         */
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        // kafka 加入 flink 源
        DataStreamSource<String> kafka = env.addSource(kafkaSource);


        //--- sink
        // kafka.print();
        kafka.addSink(new FlinkKafkaProducer<>("192.168.209.101:9092", "test-sink", new SimpleStringSchema()));


        //--- submit
        env.execute("my job.");
    }
}
