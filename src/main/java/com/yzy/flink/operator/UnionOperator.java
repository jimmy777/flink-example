package com.yzy.flink.operator;



import com.yzy.flink.source.HdfsFileSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description Union操作的例子。
 *
 * 注意这里的问题：
 * 1. 只有并行度为1了，HashMap才能被其他槽slot线程访问到！
 * 2. kafka的分区读取的，kafka的分区数为3，则只有3个槽slot并行去读取。这样其他的slot是无法接受到数据的。
 * 3. 利用 split 来判断数据来源不是一个好办法！
 *
 */
public class UnionOperator {

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
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        // kafkaSource.setStartFromLatest();
        DataStreamSource<String> kafkaInputSource = env.addSource(kafkaSource);

        // HDFS 文件源
        DataStreamSource<String> countryDictSource = env.addSource(new HdfsFileSourceFunction());


        // 两个source源进行union操作
        DataStream<String> union = countryDictSource.union(kafkaInputSource);

        /*
         * 1.union.process处理函数，processElement方法将被循环调用。
         * 2.value 是进来的每条数据。
         * 3.HashMap用来保存要比较的数据。
         */
        SingleOutputStreamOperator<String> process = union.process(new ProcessFunction<String, String>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                String[] split = value.split(",");

                if (split.length > 1) {
                    map.put(split[0], split[1]);
                    out.collect(value);
                } else {
                    String CountryName = map.get(value);
                    String outStr = CountryName == null ? "no match" : CountryName;
                    out.collect(outStr);
                }
            }
        });


        // sink
        process.print();


        // submit
        env.execute("my job.");

    }
}
