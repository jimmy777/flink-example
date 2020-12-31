package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/22 10:56
 * @Description WindowAll 例子。
 *
 * 注意：
 * 1. windowall 用于非并行处理方式。（window是并行处理方式。）
 * 2. windowall 的 KeyBy 处理未生效，因为无法并行，所以都进入一个窗口中计算了。
 * 3. fromCollection 的 source 方式，记录是有限的，到了最后一条后，程序就退出了。
 *
 */
public class ReduceFunctionOnWindowAll {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
        list.add(Tuple2.of("BJ", 1L));
        list.add(Tuple2.of("SH", 2L));
        list.add(Tuple2.of("BJ", 3L));
        list.add(Tuple2.of("SH", 4L));
        list.add(Tuple2.of("GZ", 100L));

        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(list);

        KeyedStream<Tuple2<String, Long>, String> keyedStream = input.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });


        keyedStream.countWindowAll(2).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print();


        env.execute("my job.");
    }
}
