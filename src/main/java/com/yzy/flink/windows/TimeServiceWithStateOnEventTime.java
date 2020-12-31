package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/23 9:58
 * @Description TimeService 的例子。
 *
 * 注意：
 * 1. 使用 EventTime 来处理。
 * 2. 使用 EventTime 要指定 watermark。
 * 3. 使用 registerEventTimeTimer() 来回调 onTimer() 函数。
 * 4. 使用 deleteEventTimeTimer() 清除 timer。
 *
 */
public class TimeServiceWithStateOnEventTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用 EventTime 事件时间来处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.209.101", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.209.101", 7777);

        // 数据源加入了一个时间 timestamp 作为事件时间。
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input1 = s1.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                // 注意：指定 watermark 要在 keyBy 之前来进行。
                // 指定 watermark 为 EventTime 事件时间的升序。
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                }).keyBy("f0");


        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input2 = s2.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                // 注意：指定 watermark 要在 keyBy 之前来进行。
                // 指定 watermark 为 EventTime 事件时间的升序。
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                }).keyBy("f0");



        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> connect = input1.connect(input2);
        
        connect.process(new KeyedCoProcessFunction<String,Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>() {

            // 保存记录开始时间，后面来模拟 SessionWindow 窗口功能。
            // 这个变量改为状态变量来保存。
            private ValueState<Long> dataTime = null;
            // 实现3秒钟的窗口操作。
            private final int interval = 10000;
            // 输出字符串，合并两个流的数据。
            // 这个变量要注意：在一个 subTask 中的两个 key 是共享的，即不是自己算自己的，这样是有 bug 的。
            // 这个变量改为状态变量来保存。
            private ReducingState<String> outString = null;

            // 防止每个 key 注册多个 timer
            private ValueState<Long> lastTime = null;

            // 初始化状态的值，即 datatime 和 outString 两个状态变量。
            @Override
            public void open(Configuration parameters) throws Exception {
                // 1.初始化状态值：ReducingState
                ReducingStateDescriptor<String> outStr = new ReducingStateDescriptor<>("outStr", new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1 + "\t" + value2;
                    }
                }, String.class);
                outString = getRuntimeContext().getReducingState(outStr);

                // 2.初始化值状态：ValueState
                ValueStateDescriptor<Long> vsd = new ValueStateDescriptor<>("dataTime", Long.class);
                dataTime = getRuntimeContext().getState(vsd);

                // 3.初始化值状态：ValueState
                ValueStateDescriptor<Long> vsd1 = new ValueStateDescriptor<>("lastTime", Long.class);
                lastTime = getRuntimeContext().getState(vsd1);
            }

            @Override
            public void processElement1(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                dowork(value, ctx);
            }

            private void dowork(Tuple3<String, Integer, Long> value, KeyedCoProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>.Context ctx) throws Exception {
                // 给第一个状态变量赋值。
                outString.add(value.f0);

                // 如果 lastTime 不等于空，那么说明 key 已经注册了 timer，需要删除最后一个注册的 timer。
                if (lastTime.value() != null) {
                    // 则删除最后一个 timer 操作
                    ctx.timerService().deleteEventTimeTimer(lastTime.value());
                }
                long l = value.f2;

                // 给第二个状态变量赋值。用了记录的事件时间。
                dataTime.update(l);

                // 防止同一个 key 注册多个 timer
                lastTime.update(l + interval);

                // 3秒钟之后调用，实现 TimeWindow 窗口功能。
                ctx.timerService().registerEventTimeTimer(lastTime.value());
                System.out.println("subtask_id: " + getRuntimeContext().getIndexOfThisSubtask() + ", value: " + value.f0);
            }

            @Override
            public void processElement2(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                // 给第一个状态变量赋值。
                dowork(value, ctx);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("=== 表示 timeservice 开始执行了");
                // 如果两个时间没变化，表示长时间数据无变化则触发窗口计算，即实现 SessionWindow 窗口功能。
                // 这里从两个状态变量里面取值。
                if (timestamp == dataTime.value() + interval) {
                    out.collect(outString.get());
                    // 清空操作
                    dataTime.clear();
                    lastTime.clear();
                }
            }
        }).print();


        env.execute("my job.");
    }
}
