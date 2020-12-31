package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 16:29
 * @Description AggregateFunction 窗口函数。
 * <p>
 * 注意：
 * 1. 同样可以实现 ReduceFunction 窗口函数的功能，操作更加灵活但代码稍复杂。
 * 2. 初始化定义一个累加器 ACC；
 * 3. 增量进行累加运算 ACC + Record；
 *
 */
public class ReduceWindowsFunction {

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
        keyedStream.window( org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .evictor(new Evictor<Tuple3<String, Long, Long>, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple3<String, Long, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
/*                        Iterator<TimestampedValue<Tuple3<String, Long, Long>>> iterator = elements.iterator();
                        for (TimestampedValue<Tuple3<String, Long, Long>> next : elements) {
                            System.out.println("before: " + next.getValue());
                            if (next.getValue().f2 % 5 == 0) {
                                iterator.remove();
                            }
                        }*/

                        Iterator<TimestampedValue<Tuple3<String, Long, Long>>> iterator = elements.iterator();

                        while (iterator.hasNext()) {
                            Tuple3<String, Long, Long> value = iterator.next().getValue();
                            System.out.println("before: " + value);
                            if (value.f2 % 5 == 0) {
                                iterator.remove();
                            }
                        }
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple3<String, Long, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
/*                        for (TimestampedValue<Tuple3<String, Long, Long>> next : elements) {
                            System.out.println("after: " + next.getValue());
                        }*/

                        Iterator<TimestampedValue<Tuple3<String, Long, Long>>> iterator = elements.iterator();

                        while (iterator.hasNext()) {
                            Tuple3<String, Long, Long> value = iterator.next().getValue();
                            System.out.println("after: " + value);
                        }
                    }
                })
                // ReduceFunction() 函数
/*                .reduce((ReduceFunction<Tuple3<String, Long, Long>>) (value1, value2) -> {
                    // 对元组的第三个进行累加求和计算。
                    return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                }).print();*/

                //
                .aggregate(new AggregateFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> createAccumulator() {
                        // 创建一个累加器，负责保存计算结果的。
                        // 定义累加器的初始值。
                        // 第一次运算时使用。
                        return Tuple3.of("", 0L, 0L);
                    }

                    @Override
                    public Tuple3<String, Long, Long> add(Tuple3<String, Long, Long> value, Tuple3<String, Long, Long> accumulator) {
                        // add() 累加
                        // accumulator 为累加器，上一次计算的结果保存在了这里。
                        return Tuple3.of(value.f0, value.f1, value.f2 + accumulator.f2);
                    }

                    @Override
                    public Tuple3<String, Long, Long> getResult(Tuple3<String, Long, Long> accumulator) {
                        // accumulator 就是返回计算结果了。
                        return accumulator;
                    }

                    @Override
                    public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> a, Tuple3<String, Long, Long> b) {
                        return Tuple3.of(a.f0, a.f1, a.f2 + b.f2);
                    }
                }).print();



        //--- 注意: sink 并不是 action，Flink 没有 sink 也能提交任务去跑。只要有下面的语句 env.execute() 就可以。

        //--- submit
        env.execute("my job.");
    }
}
