package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description 窗口处理函数。
 * <p>
 * 注意：
 * 1. AggregateFunction 窗口函数
 * 2. reduce() operator函数，可以传入两个窗口处理函数，一般这样第二个参数用于获取窗口的状态。第一个ReduceFunction处理后，再由第二个WindowFunction处理。
 */
public class ProcessFunctions {

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
        keyedStream.print();
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))

// 方式一： 不带获取窗口状态的实现。reduce 与 aggregate 实现累加。
/*            .aggregate(new AggregateFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                   @Override
                   public Tuple2<String, Long> createAccumulator() {
                       return Tuple2.of("", 0L);
                   }

                   @Override
                   public Tuple2<String, Long> add(Tuple3<String, Long, Long> value, Tuple2<String, Long> accumulator) {
                       return Tuple2.of(value.f0, value.f2 + accumulator.f1);
                   }

                   @Override
                   public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                       return accumulator;
                   }

                   @Override
                   public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                       return Tuple2.of(a.f0, a.f1 + b.f1);
                   }
               }
            ).print();*/
/*
            .apply(new WindowFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Long, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                    Iterator<Tuple3<String, Long, Long>> iterator = input.iterator();

                    Long sum = 0L;
                    while (iterator.hasNext()) {
                        Tuple3<String, Long, Long> next = iterator.next();
                        Long f2 = next.f2;
                        sum += f2;
                    }

                    out.collect(Tuple2.of(s, sum));
                }
            }).print();
*/

// 方式二： 带获取窗口状态的实现。reduce 与 aggregate 实现累加。
/*            .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                // 1. 先传给 reduce() 函数，处理结果给第二个参数。注意，这里是reduce()函数累加计算剩一条结果后传给了WindowFunction去计算。
                @Override
                public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
                    return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                }
            }, new WindowFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                // 2. 第二个 WindowFunction() 函数 进一步进行处理，这个窗口函数可以得到本窗口的状态值。
                @Override
                public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Long, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                    System.out.println(
                            "--> (process) subtask: " + window.getStart() + "\t" +
                                    window.getEnd() + "\t" +
                                    window.getClass()
                    );

                    Iterator<Tuple3<String, Long, Long>> iterator = input.iterator();
                    Tuple3<String, Long, Long> next = iterator.next();
                    out.collect(Tuple2.of(s, next.f2));
                }
            }).print();*/

            .aggregate(
                    new AggregateFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> createAccumulator() {
                            return Tuple2.of("", 0L);
                        }

                        @Override
                        public Tuple2<String, Long> add(Tuple3<String, Long, Long> value, Tuple2<String, Long> accumulator) {
                            return Tuple2.of(value.f0, value.f2 + accumulator.f1);
                        }

                        @Override
                        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                            return accumulator;
                        }

                        @Override
                        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                            return Tuple2.of(a.f0, a.f1 + b.f1);
                        }
                    },
                    new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                        @Override
                        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {

                            System.out.println(
                                    "--> (process) subtask: " + getRuntimeContext().getIndexOfThisSubtask() +
                                            "| start: " + context.window().getStart() +
                                            "| end: " + context.window().getEnd() +
                                            "| watermark: " + context.currentWatermark() +
                                            "| currentTime: " + System.currentTimeMillis()
                            );

                            Iterator<Tuple2<String, Long>> iterator = elements.iterator();
                            Tuple2<String, Long> next = iterator.next();
                            System.out.println(next);
                            out.collect(Tuple2.of(s, next.f1));
                        }
                    }

            ).print();


        //--- submit
        env.execute("my job.");
    }
}
