package com.yzy.flink.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/23 15:35
 * @Description
 */
public class IntCounterUnboundedWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.209.101", 6666);

        SingleOutputStreamOperator<Integer> reduce = stringDataStreamSource.flatMap(new RichFlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {

                String[] words = value.split(" ");

                for (String s : words) {
                    out.collect(1);
                }

            }
        }).timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        });

        Iterator<Integer> collect = DataStreamUtils.collect(reduce);
        for (;collect.hasNext();) {
            Integer next = collect.next();

            // 这里可以插入数据库，并且是统计好的值。
            System.out.println("count: " + next);
        }

        env.execute("my job.");
    }
}
