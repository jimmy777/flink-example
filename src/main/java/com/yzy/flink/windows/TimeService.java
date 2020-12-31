package com.yzy.flink.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/23 9:58
 * @Description TimeService 的例子。
 *
 * 作用：
 * 1. 这个操作比较底层，同样可以实现 TimeWindow 窗口和 SessionWindow 窗口功能。
 * 2. onTimer() 方法用于回调。
 *
 */
public class TimeService {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("192.168.209.101", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.209.101", 7777);

        KeyedStream<Tuple2<String, Integer>, Tuple> input1 = s1.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy("f0");
        KeyedStream<Tuple2<String, Integer>, Tuple> input2 = s2.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy("f0");

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connect = input1.connect(input2);
        
        connect.process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {

            // 保存记录开始时间，后面来模拟 SessionWindow 窗口功能。
            private Long datetime = null;
            // 实现3秒钟的窗口操作。
            private final int interval = 10000;
            // 输出字符串，合并两个流的数据。
            // 这个变量要注意：在一个 subTask 中的两个 key 是共享的，即不是自己算自己的，这样是有 bug 的。
            private String outString = "";

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                long l = System.currentTimeMillis();
                datetime = l;
                // 3秒钟之后调用，实现 TimeWindow 窗口功能。
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subtask_id: " + getRuntimeContext().getIndexOfThisSubtask() + ", value: " + value.f0);
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                long l = System.currentTimeMillis();
                datetime = l;
                ctx.timerService().registerProcessingTimeTimer(l + interval);
                System.out.println("subtask_id: " + getRuntimeContext().getIndexOfThisSubtask() + ", value: " + value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("=== 表示 timeservice 开始执行了");
                // 如果两个时间没变化，表示长时间数据无变化则触发窗口计算，即实现 SessionWindow 窗口功能。
                if (timestamp == datetime + interval) {
                    out.collect(outString);
                    outString = "";
                }
            }
        }).print();


        env.execute("my job.");
    }
}
