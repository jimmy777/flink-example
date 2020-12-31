package com.yzy.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description EventTime 时间，内置 Watermark API。
 *
 * AscendingTimestampExtractor 内置 API 生成发送的 Watermark 时间。
 *
 * 注意：
 * 1. 数据升序场景下。
 * 2. 只要获取 EventTime 时间即可，Watermark 自动生成。
 *
 */
public class TimestampWatermark4 {

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


        // 内置 API 实现方法一： AscendingTimestampExtractor 对象，基于 EventTime 来自动生成 Watermark 时间。
        // 场景是数据是升序的。
        SingleOutputStreamOperator<String> datastream = dss.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
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
