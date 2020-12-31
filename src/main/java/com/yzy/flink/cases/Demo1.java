package com.yzy.flink.cases;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description
 */
public class Demo1 {

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 在这里设置将影响下面所有的窗口

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
        kafkaSource.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<String>) new CustomWatermarkEmitter());
        // kafka 加入 flink 源
        SingleOutputStreamOperator<String> apply = env.addSource(kafkaSource).windowAll(TumblingProcessingTimeWindows.of(Time.hours(1), Time.minutes(15))).apply(new AllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<String> values, Collector<String> out) throws Exception {
                for (String s : values) {
                    out.collect(s);
                }
            }
        });

/*        SingleOutputStreamOperator<Tuple2<String, Double>> keyedStream = kafka.flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(300)).apply(new WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Double>> input, Collector<Tuple2<String, Double>> out) throws Exception {
                double sum = 0L;
                int count = 0;

                for (Tuple2<String, Double> record : input) {
                    sum += record.f1;
                    count++;
                }

                Tuple2<String, Double> next = input.iterator().next();

                next.f1 = (sum / count);
                out.collect(next);
            }
        });*/


        //--- sink
        apply.print();


        //--- submit
        env.execute("my job.");
    }

    private static class CustomWatermarkEmitter implements org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<String>, org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<String> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            System.out.println("----- 进入 watermark");
            return null;
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {

            if (null != lastElement && lastElement.contains(",")) {
                String[] split = lastElement.split(",");
                return new Watermark(Long.parseLong(split[0]));
            }

            return null;
        }

        /**
         *
         * 提取时间戳
         */
        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            if (null != element && element.contains(",")) {
                String[] split = element.split(",");
                System.out.println("------ watermark: timestamp=" + split[0]);
                return Long.parseLong(split[0]);
            }

            return 0;
        }
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String, String>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            if(StringUtils.isNullOrWhitespaceOnly(value)) {
                System.out.println("invalid line");
                return;
            }

            for(String word: value.split(",")) {
                out.collect(new Tuple2<>(word, value));
            }
        }
    }
}
