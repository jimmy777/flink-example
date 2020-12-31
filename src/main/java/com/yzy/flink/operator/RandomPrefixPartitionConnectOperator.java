package com.yzy.flink.operator;



import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description 自定义 Partition 方法进行分区 -> connect 处理。
 *
 * 记录加上 random 随机前缀。
 * 可以解决一类数据倾斜问题。
 *
 * 注意：
 * 1. 不使用 keyBy 的 hashCode 来分区处理，自己制定控制分区。
 * 2. 自己写一个类实现 Partitioner 接口，实现 partition() 函数，返回分区的数。
 * 3. 两个 DataStream 数据流要使用同一个自定义的 Partition 实现类来处理分区，即使用相同的分区规则。
 *
 */
public class RandomPrefixPartitionConnectOperator {

    public static void main(String[] args) throws Exception {

        //-------------------- env --------------------//
        // 创建本地环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 注意：有了keyBy的处理，并行度可以不设置为1了！
        // env.setParallelism(1);


        //-------------------- kafka DataStream --------------------//
        // 配置一些 kafka 服务的属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");

        // Kafka source源。自定义的MyKafkaRecord格式。
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // kafkaSource.setStartFromLatest();
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);

        // map 操作。
        // 对每条记录加上随机前缀 random。
        SingleOutputStreamOperator<MyKafkaRecord> kafkaSourceMap = kafkaInputSource.map(new MapFunction<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord map(MyKafkaRecord value) throws Exception {
                String record = value.getRecord();

                Random random = new Random();
                int tag = random.nextInt(8);

                return new MyKafkaRecord(tag + "_" + record);
            }
        });

        // 对 kafka 的 DataStream 使用自定义的 partition。
        DataStream<MyKafkaRecord> kafkaRecordDataStream = kafkaSourceMap.partitionCustom(new CustomRandomPrefixPartitioner(), new KeySelector<MyKafkaRecord, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(MyKafkaRecord value) throws Exception {
                return value;
            }
        });


        //-------------------- hdfs DataStream --------------------//
        // HDFS 文件源。先用map处理一下数据，把String记录拆分后转换为Tuple元组(key,value)。
        DataStreamSource < String > countryDictSource = env.addSource(new HdfsFileSourceFunction());

        // 对记录进行 map 处理后，在根据自定义的 partition 进行分区。
        DataStream<Tuple2<MyKafkaRecord, String>> hdfsTuple2DataStream = countryDictSource.map(new MapFunction<String, Tuple2<MyKafkaRecord, String>>() {

            // String转换为Tuple类型。
            @Override
            public Tuple2<MyKafkaRecord, String> map(String value) throws Exception {
                String[] split = value.split(",");

                Random random = new Random();
                int tag = random.nextInt(8);

                MyKafkaRecord key = new MyKafkaRecord(tag + "_" + split[0]);

                return Tuple2.of(key, split[1]);
            }
        }).partitionCustom(new CustomRandomPrefixPartitioner(), new KeySelector<Tuple2<MyKafkaRecord, String>, MyKafkaRecord>() {
            @Override
            public MyKafkaRecord getKey(Tuple2<MyKafkaRecord, String> value) throws Exception {
                return value.f0;
            }
        });


        //-------------------- operator --------------------//
        // connect 连接操作，即相同的key在一起处理。
        ConnectedStreams<Tuple2<MyKafkaRecord, String>, MyKafkaRecord> connect = hdfsTuple2DataStream.connect(kafkaRecordDataStream);

        // connect 处理逻辑。
        // 左边是hdfs（Tuple类型），右边是kafka（自定义的MyKafkaRecord类型）。
        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Tuple2<MyKafkaRecord, String>, MyKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<>();

            // 左边是 hdfs 数据流。
            @Override
            public void processElement1(Tuple2<MyKafkaRecord, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0.getRecord(), value.f1);
                out.collect(value.toString()); // 发射是为了看看验证一下
            }

            // 右边是 kafka 数据流。
            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                String countryName = map.get(value.getRecord()); // 使用 MyKafkaRecord 对象值来从 map 中取 value。
                String outStr = countryName == null ? "no match" : countryName;
                // 处理结果发射出去
                out.collect(outStr);
            }
        });


        //-------------------- sink --------------------//
        process.print();


        //-------------------- submit --------------------//
        env.execute("my job.");
    }
}