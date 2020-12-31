package com.yzy.flink.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description 基于 EventTime 时间，内置 Watermark API。
 *
 * BoundedOutOfOrdernessTimestampExtractor 内置 API 发送 watermark。
 *
 * 注意：
 * 1. 数据乱序场景下。
 * 2. 其构造参数为 EventTime 将要减掉的时间（即延时的时间s），来得到 Watermark 时间。
 *
 */
public class TimestampWatermark5 {

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


        // 内置 API 实现方法二： BoundedOutOfOrdernessTimestampExtractor 对象，基于延时时间来自动生成 Watermark 时间。
        // 场景是数据是乱序的。
        SingleOutputStreamOperator<String> datastream = dss.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String element) {
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
