package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description 基于 EventTime 时间，在 source 端来发送 watermark。
 *
 * 注意：
 * 1. print() 端可以看到收到的 watermark 值。
 * 2. 每个 subTask 都会收到 source 端发来的 watermark 值。
 * 3. watermark 值要比 eventtime 的值要小，不然事件总是总是被丢弃。
 *
 */
public class TimestampWatermark1 {

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 指定使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //--- source
        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            private boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    int i = new Random().nextInt(10) + 30;
                    String msg = currentTimeMillis + "," + i + ",001";


                    String[] split = msg.split(",");
                    long timestamp = Long.parseLong(split[0]);
                    String data  = split[1] + "," + split[2];


                    ctx.collectWithTimestamp(data, timestamp);
                    // ctx.collect(msg);
                    ctx.emitWatermark(new Watermark(currentTimeMillis - 1000));

                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = true;
            }
        });


        //--- sink
        stringDataStreamSource.print();


        //--- submit
        env.execute("my job.");
    }
}
