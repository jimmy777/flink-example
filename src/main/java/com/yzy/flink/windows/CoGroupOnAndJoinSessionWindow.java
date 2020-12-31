package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/22 11:16
 * @Description CoGroup 和 join 例子。
 * <p>
 * 二者区别：
 * CoGroup 关联不上的记录需要补齐。
 * join 即关联上的记录。
 * <p>
 * CoGroup：
 * 1. 需要先 KeyBy 处理，有了 key 才能 join 处理。
 * 2. A.coGroup(B)，后使用 where() 处理 A 中的哪个 key 和 equalTo() 处理 B 中的哪个 key 相等，实现关联。
 * 3. CoGroupFunction() 窗口处理函数。
 */
public class CoGroupOnAndJoinSessionWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("192.168.209.101", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.209.101", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> input1 = s1.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> input2 = s2.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 方法一：CoGroupFunction 窗口处理函数。
        /*
        input1.coGroup(input2).where(new KeySelector<Tuple2<String, Integer>, String>() {
            // 使用 input1 的哪一个列值（即key）来关联。
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
            // equalTo() 用来处理等值运算的，即要关联的 input2 的列（即key）。
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
                // 定义一个 session 窗口，10秒钟没有操作触发运算。
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                // 触发器，来一条记录就触发 apply 运算。
        .trigger(CountTrigger.of(1))
        .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
            @Override
            public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {
                StringBuilder sb = new StringBuilder();

                // 下面看出，左右两边都是一堆的数据，进行迭代处理。

                sb.append("Data in stream1: \n");

                Iterator<Tuple2<String, Integer>> iterator = first.iterator();
                for (;iterator.hasNext();) {
                    Tuple2<String, Integer> next = iterator.next();
                    sb.append(next.f0).append("<->").append(next.f1).append("\n");
                }

                sb.append("Data in stream2: \n");

                Iterator<Tuple2<String, Integer>> iterator2 = second.iterator();
                for (;iterator2.hasNext();) {
                    Tuple2<String, Integer> next = iterator2.next();
                    sb.append(next.f0).append("<->").append(next.f1).append("\n");
                }

                // 处理结果发射出去
                out.collect(sb.toString());
            }
        }).print();*/

        // 方法二：join() 后，使用 JoinFunction 窗口处理函数。
        input1.join(input2).where(new KeySelector<Tuple2<String, Integer>, String>() {
            // 使用 input1 的哪一个列值（即key）来关联。
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
            // equalTo() 用来处理等值运算的，即要关联的 input2 的列（即key）。
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
        // 定义一个 session 窗口，10秒钟没有操作触发运算。
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
        // 触发器，来一条记录就触发 apply 运算。
        .trigger(CountTrigger.of(1))
        .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
            // 单条记录进行处理
            @Override
            public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                return first.f0 + "==" + second.f0;
            }
        }).print();


        env.execute("my job.");
    }
}
