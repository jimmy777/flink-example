package com.yzy.flink.windows;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/23 15:01
 * @Description 有界数据流的累加器的例子。
 *
 * 注意：
 * 1. IntCounter() 定义一个累加器；
 * 2. addAccumulator() 注册一个累加器，带名称。
 * 3. getAccumulatorResult() 获取一个累加器结果，根据注册的名称。
 *
 */
public class IntCounterBounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.fromElements("a a a", "b b b");

        stringDataStreamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            private IntCounter inc = null;


            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("word", this.inc);
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split(" ");

                for (String s: ss) {
                    this.inc.add(1);
                    out.collect(Tuple2.of(s, 1));
                }

            }
        }).print();

        JobExecutionResult execute = env.execute("my job.");
        Integer num = execute.getAccumulatorResult("word");
        System.out.println(num);
    }
}
