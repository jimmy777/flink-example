package com.yzy.flink.operator;



import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description keyBy操作，并且使用自定义的POJO类作为Key来处理。
 *
 * 注意：
 * 1. POJO类要有HashCode()实现，比较判断是否key相同；
 *
 */
public class HashKeybyConnectOperator {

    public static void main(String[] args) throws Exception {

        // 创建本地环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 注意：有了keyBy的处理，并行度可以不设置为1了！
        // env.setParallelism(1);

        // 配置一些 kafka 服务的属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");

        // Kafka source源。自定义的MyKafkaRecord格式。
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // kafkaSource.setStartFromLatest();
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);
        // 对kafka的DataStream进行keyBy操作。
        KeyedStream<MyKafkaRecord, MyKafkaRecord> kafkaRecordStringKeyedStream = kafkaInputSource.keyBy(new KeySelector<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(MyKafkaRecord value) throws Exception {
                return value;
            }
        });

        // HDFS 文件源。先用map处理一下数据，把String记录拆分后转换为Tuple元组(key,value)。然后再进行KeyBy处理。
        DataStreamSource<String> countryDictSource = env.addSource(new HdfsFileSourceFunction());

        // 注意，Tuple元组中key为自定义的MyKafkaRecord类型。
        KeyedStream<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> HdfsRecordStringKeyedStream = countryDictSource.map(new MapFunction<String, Tuple2<MyKafkaRecord, String>>() {

            // String转换为Tuple类型。
            @Override
            public Tuple2<MyKafkaRecord, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(new MyKafkaRecord(split[0]), split[1]); // 注意MyKafkaRecord的初始化。
            }
        }).keyBy(new KeySelector<Tuple2<MyKafkaRecord, String>, MyKafkaRecord>() {
            // 第一个字段作为为Key。
            @Override
            public MyKafkaRecord getKey(Tuple2<MyKafkaRecord, String> value) throws Exception {
                return value.f0;
            }
        });

        // keyedStream 连接，即相同的key在一起处理。使用MyKafkaRecord对象的hashCode函数来比较判断key是否相同。
        ConnectedStreams<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> connect = HdfsRecordStringKeyedStream.connect(kafkaRecordStringKeyedStream);

        // 处理逻辑。左边是hdfs（Tuple类型），右边是kafka（自定义的MyKafkaRecord类型）。
        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<MyKafkaRecord, Tuple2<MyKafkaRecord, String>, MyKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<>();

            // 左边hdfs数据流。
            @Override
            public void processElement1(Tuple2<MyKafkaRecord, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0.getRecord(), value.f1); // 注意：是把MyKafkaRecord的值存入了map保存。
                out.collect(value.toString()); // 发射是为了看看验证一下
            }

            // 右边kafka数据流。
            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                // 两种方法：
                // String countryName = map.get(value.getRecord());
                String countryName = map.get(ctx.getCurrentKey().getRecord()); // 注意：key是MyKafkaRecord类型，需要getRecord后的值去map里面找对应的value。

                String outStr = countryName == null ? "no match" : countryName;
                // 处理结果发射出去
                out.collect(outStr);

            }
        });


        // sink
        process.print();


        // submit
        env.execute("my job.");

    }
}
