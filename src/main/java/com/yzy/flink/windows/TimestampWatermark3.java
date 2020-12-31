package com.yzy.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
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
public class TimestampWatermark3 {

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 指定使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //--- source
        DataStreamSource<String> dss = env.addSource(new SourceFunction<String>() {

            private boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    long currentTimeMillis = System.currentTimeMillis();
                    int i = new Random().nextInt(10) + 30;
                    String msg = currentTimeMillis + "," + i + ",001";

                    ctx.collect(msg);
                    // 休息1秒，即1秒发送一次数据。
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = true;
            }
        });


        // 指定发送 watermark 的方式有两种：一种基于时间触发的，另一种是基于事件触发的。
        SingleOutputStreamOperator<String> datastream = dss.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {

            /**
             * 基于事件的 Watermark。判断记录中是否满足某些条件时，触发发送 Watermark 值。
             *
             * @param lastElement 最后一条事件记录了，进行判断。
             * @param extractedTimestamp 注意：这个就是下面的函数传递过来的 EventTime 的值，所以就不用自己在切分得到了。
             * @return
             */
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                String[] split = lastElement.split(",");
                if ("002".equals(split[2])) {
                    return new Watermark(extractedTimestamp - 1000);
                } else {
                    return null;
                }
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split(",");

                return Long.parseLong(split[0]);
            }
        });


        //--- sink
        datastream.print();


        //--- submit
        env.execute("my job.");
    }
}
