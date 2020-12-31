package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description Session Windows 窗口的例子。
 * <p>
 * 注意：
 * 1. 窗口大小是不固定的。
 * 2. 根据 session 时间来触发窗口计算，分两种情况： 固定 Gap 和 动态 Gap。
 * 3. 使用 EventTime 得有 Watermark 处理，来触发窗口计算。
 */
public class SessionWindows {

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

                Long num = 0L;

                while (isCancel) {
                    num++;
                    // 模拟购物 session 时间，有长有短
                    if (num % 5 == 0) {
                        // 多睡一会儿。3秒超过下面的2秒session间隔。
                        Thread.sleep(3000);
                    } else {
                        // 少睡一会儿
                        Thread.sleep(1000);
                    }
                    ctx.collect(Tuple3.of("CH", System.currentTimeMillis(), num));
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

        // Session Windows 操作
        // 方式一：固定 Gap。超过2秒触发。
/*        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(2)))
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
            }).print();*/

        // 方式二：动态 Gap。
        keyedStream.window(EventTimeSessionWindows.withDynamicGap((SessionWindowTimeGapExtractor<Tuple3<String, Long, Long>>) element -> {

            if (element.f2 % 5 == 0) {
                return 2500L;
            } else {
                return 2000L;
            }

        })).process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
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
