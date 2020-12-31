package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/23 9:58
 * @Description TimeService 的例子。
 *
 * 注意：
 * 1. 带状态的变量，实现在 同一个 subTask 中，不同的 key 也可以各算各的。
 * 2. 状态要在 open() 方法中进行初始化。
 * 3. 后面处理函数，可以从状态变量中获取值。
 * 4. 学习两种状态变量的类型： ValueState 和 ReducingState。
 *
 */
public class TimeServiceWithState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("192.168.209.101", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.209.101", 7777);

        KeyedStream<Tuple2<String, Integer>, Tuple> input1 = s1.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy("f0");
        KeyedStream<Tuple2<String, Integer>, Tuple> input2 = s2.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy("f0");

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connect = input1.connect(input2);
        
        connect.process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {

            // 保存记录开始时间，后面来模拟 SessionWindow 窗口功能。
            // 这个变量改为状态变量来保存。
            private ValueState<Long> datatime = null;
            // 实现3秒钟的窗口操作。
            private final int interval = 10000;
            // 输出字符串，合并两个流的数据。
            // 这个变量要注意：在一个 subTask 中的两个 key 是共享的，即不是自己算自己的，这样是有 bug 的。
            // 这个变量改为状态变量来保存。
            private ReducingState<String> outString = null;

            // 初始化状态的值，即 datatime 和 outString 两个状态变量。
            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态值：ReducingState
                ReducingStateDescriptor<String> outStr = new ReducingStateDescriptor<>("outStr", new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1 + "\t" + value2;
                    }
                }, String.class);
                outString = getRuntimeContext().getReducingState(outStr);

                // 初始化值状态：ValueState
                ValueStateDescriptor<Long> vsd = new ValueStateDescriptor<>("time", Long.class);
                datatime = getRuntimeContext().getState(vsd);
            }

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                // 给第一个状态变量赋值。
                outString.add(value.f0);

                // 给第二个状态变量赋值。
                long l = System.currentTimeMillis();
                datatime.update(l);

                // 3秒钟之后调用，实现 TimeWindow 窗口功能。
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subtask_id: " + getRuntimeContext().getIndexOfThisSubtask() + ", value: " + value.f0);
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                // 给第一个状态变量赋值。
                outString.add(value.f0);

                // 给第二个状态变量赋值。
                long l = System.currentTimeMillis();
                datatime.update(l);
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subtask_id: " + getRuntimeContext().getIndexOfThisSubtask() + ", value: " + value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("=== 表示 timeservice 开始执行了");
                // 如果两个时间没变化，表示长时间数据无变化则触发窗口计算，即实现 SessionWindow 窗口功能。
                // 这里从两个状态变量里面取值。
                if (timestamp == datatime.value() + interval) {
                    out.collect(outString.get());
                    // 清空操作
                    datatime.clear();
                }
            }
        }).print();


        env.execute("my job.");
    }
}
