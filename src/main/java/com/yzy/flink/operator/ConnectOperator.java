package com.yzy.flink.operator;



import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description 两个流的 Connected 操作。
 *
 * 注意这里的问题：
 * 1. 只有并行度为1了，HashMap才能被其他槽slot线程访问到！
 * 2. kafka的分区读取的，kafka的分区数为3，则只有3个槽slot并行去读取。这样其他的slot是无法接受到数据的。
 * 3. 利用 split 来判断数据来源不是一个好办法！
 *
 */
public class ConnectOperator {

    public static void main(String[] args) throws Exception {

        // 创建本地环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 注意：这里设置了并行度为1，否则不能保证所有槽slot都能访问HashMap的数据。
        env.setParallelism(1);

        // 配置一些 kafka 服务的属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");

        // Kafka source源
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // kafkaSource.setStartFromLatest();
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);

        // HDFS 文件源
        DataStreamSource<String> countryDictSource = env.addSource(new HdfsFileSourceFunction());


        /*
         * 1.connect.process处理函数，覆盖两个方法：processElement1（左边的流）和processElement2（右边的流）。
         * 2.CoProcessFunction<左边的类型，右边的类型，返回值的类型>
         * 3.HashMap用来保存要比较的数据。
         */
        // 两个source源进行connect操作。左边是hdfs，右边是kafka。
        ConnectedStreams<String, MyKafkaRecord> connect = countryDictSource.connect(kafkaInputSource);

        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<String, MyKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<>();

            // 左边，这个流是处理hdfs文件的。
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                // 对value进来的数据进行分割
                String[] split = value.split(",");
                // 分割结果put存入map
                map.put(split[0], split[1]);
                // 处理结果发射出去
                out.collect(value);
            }

            // 右边，这个流是处理kafka的。
            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                // 从map中get获取匹配的数据。
                String CountryName = map.get(value.getRecord());
                String outStr = CountryName == null ? "no match" : CountryName;
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
