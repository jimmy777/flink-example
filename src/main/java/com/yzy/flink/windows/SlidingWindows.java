package com.yzy.flink.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description Sliding Windows 滑动窗口的例子。
 * <p>
 * 注意：
 * 1. 固定时间大小的窗口。
 * 2. 根据滑动时间来触发窗口计算，这样数据就有可能重叠。
 *
 */
public class SlidingWindows {

    public static void main(String[] args) throws Exception {

        //--- env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 指定使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //--- source
        KeyedStream<Tuple3<String, Long, Long>, String> keyedStream = env.addSource(new SourceFunction<Tuple3<String, Long, Long>>() {

            private boolean isCancel = true;

            /**
             * source 源模拟产生元组记录：如：(CH,1607654263504,1)
             */
            @Override
            public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {

                Long num = 1L;

                while (isCancel) {
                    num++;
                    ctx.collect(Tuple3.of("CH", System.currentTimeMillis(), num));

                    // 睡一秒钟后发送记录
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
                // 时间戳为 Watermark 时间，即元组的f1字段，如：1607654263504
                return element.f1;
            }
        }).keyBy((KeySelector<Tuple3<String, Long, Long>, String>) value -> {
            // 分区 key 为元组的 f0 字段，如"CH"
            return value.f0;
        });

        // Sliding Windows 操作
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .reduce((ReduceFunction<Tuple3<String, Long, Long>>) (value1, value2) -> Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2)).print();
            .process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
                @Override
                public void process(String s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                    System.out.println(
                            "--> (process) subtask: " + getRuntimeContext().getIndexOfThisSubtask() +
                                    "| start: " + context.window().getStart() +
                                    "| end: " + context.window().getEnd() +
                                    "| watermark: " + context.currentWatermark() +
                                    "| currentTime: " + System.currentTimeMillis()
                    );

                    for (Tuple3<String, Long, Long> next : elements) {
                        out.collect(next);
                    }
                }
            }).print();

        //--- 注意: sink 并不是 action，Flink 没有 sink 也能提交任务去跑。只要有下面的语句 env.execute() 就可以。

        //--- submit
        env.execute("my job.");
    }
}
