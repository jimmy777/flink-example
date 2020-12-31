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
 * @Description 无界数据流的累加器的例子。
 *
 * 注意：
 * 1. IntCounter() 定义一个累加器；
 * 2. 由于 execute() 方法会阻塞，程序不会退出，所以无法得到 IntCounter 累加器的值。
 * 3. 可以将 IntCounter 累加器的值作为结果输出 sink， 再保存起来使用，如 redis。
 *
 */
public class IntCounterUnbounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1，因为累加器在每个 subtask 分别计算。
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.209.101", 6666);

        stringDataStreamSource.flatMap(new RichFlatMapFunction<String, IntCounter>() {

            private IntCounter inc = null;


            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("word", this.inc);
            }

            @Override
            public void flatMap(String value, Collector<IntCounter> out) throws Exception {
                String[] ss = value.split(" ");

                for (String s: ss) {
                    this.inc.add(1);
                    out.collect(this.inc);
                }

            }
            // 可以自己写一个自定义的 sink 把 IntCounter 累加器的值保存起来， 如 redis。
        }).print();

        // 执行的时候会阻塞的
        env.execute("my job.");
    }
}
