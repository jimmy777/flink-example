package com.yzy.flink.sink;



import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description Socket Sink 实现。
 *
 * 注意：
 * 1. 分流器结果数据写入 Socket。
 * 2. socket 是并行度为1。
 *
 */
public class SocketSink {

    // 定义分流器，类型为 String。再起个名字。
    private static final OutputTag<String> ot = new OutputTag<String>("Anguilla"){};

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

        // Kafka DataSource 源。
        // 每条记录使用自定义的 MyKafkaRecord 格式对象。
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // kafkaSource.setStartFromLatest();
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);
        // 对kafka的DataStream进行keyBy操作。
        KeyedStream<MyKafkaRecord, String> kafkaRecordStringKeyedStream = kafkaInputSource.keyBy(new KeySelector<MyKafkaRecord, String>() {
            @Override
            public String getKey(MyKafkaRecord value) throws Exception {
                return value.getRecord();
            }
        });


        //-------------------- hdfs DataStream --------------------//
        // HDFS DataSource 源。
        // 每条记录是 String 文本。
        DataStreamSource<String> countryDictSource = env.addSource(new HdfsFileSourceFunction());

        // 对每条记录先 map 处理，把String记录拆分后转换为Tuple元组(key,value)。再按 key 进行分区处理。
        KeyedStream<Tuple2<String, String>, String> HdfsRecordStringKeyedStream = countryDictSource.map(new MapFunction<String, Tuple2<String, String>>() {

            // String转换为Tuple类型。
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            // 第一个字段作为为Key。
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });


        //-------------------- operator --------------------//
        // （两个 keyedStream 数据流）connect 连接操作，即相同的key在一起处理。
        ConnectedStreams<Tuple2<String, String>, MyKafkaRecord> connect = HdfsRecordStringKeyedStream.connect(kafkaRecordStringKeyedStream);

        // connect 处理逻辑。
        // 左边是hdfs（Tuple类型），右边是kafka（自定义的MyKafkaRecord类型）。
        SingleOutputStreamOperator<Tuple2<String, Integer>> connectInput = connect.process(new KeyedCoProcessFunction<String, Tuple2<String, String>, MyKafkaRecord, Tuple2<String, Integer>>() {

            private Map<String, String> map = new HashMap<>();

            // 左边是 hdfs 数据流。
            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                map.put(value.f0, value.f1);
                out.collect(Tuple2.of(value.toString(), 3)); // 发射是为了看看验证一下
            }

            // 右边是 kafka 数据流。
            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 两种方法：
                // String countryName = map.get(value.getRecord());
                String countryName = map.get(ctx.getCurrentKey());

                String outStr = countryName == null ? "no match" : countryName;

                // 分流器
                if(outStr.contains("安哥拉")) {
                    // 抛出数据 outStr
                    ctx.output(ot, outStr);
                }

                // 处理结果发射出去
                out.collect(Tuple2.of(value.getRecord(), 2));

            }
        });


        // keyBy() -> min()
        SingleOutputStreamOperator<Tuple2<String, Integer>> min = connectInput.keyBy(0).min(1);


        // 获取分流的数据
        DataStream<String> sideOutput = connectInput.getSideOutput(ot);
        // 分流数据写入 socket。
        sideOutput.writeToSocket("192.168.209.101", 6666, new SimpleStringSchema());

        //-------------------- sink --------------------//
        min.print();


        //-------------------- submit --------------------//
        env.execute("my job.");

    }
}
