package com.yzy.flink.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/19 14:32
 * @Description 利用socket源来学习各种operator实现方式，source(socket) -> flatmap -> map -> keyBy -> print(sink)。
 *
 * 五种operator实现方式：
 * 1. 写 lambda 表达式；
 * 2. 写 内部实现类 Function；
 * 3. flatMap 与 map 组合实现；
 * 4. RichFlatMap 实现，功能稍多一些；
 * 5. 利用 原始的PrcessFunction类自己写 flatMap 功能。
 *
 */
public class SocketWordCount {

    public static void main(String[] args) throws Exception {

        // 创建本地环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 配置source源为socket
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666, "\n");


        /* 写法（一），lambda 表达式 */
        // word 随机发送，负载均衡。
        /*
        SingleOutputStreamOperator<String> flatMap = socket.flatMap((String value, Collector<String> out) -> {
            Arrays.stream(value.split(" ")).forEach(word -> {
                out.collect(word);
            });
        }).returns(Types.STRING);


        // 元组 (word,1) 随机发送，负载均衡。
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 元组 (word, count) 按照 key 值来发送、进行累加。
        SingleOutputStreamOperator <Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

         */

        /* 写法（二），内部实现类 Function */
        /*
        // flatMap 实现，发射字符串 word
        SingleOutputStreamOperator<String> flatMap = socket.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                String[] s = value.split(" ");
                for (String ss : s) {
                    out.collect(ss);
                }
            }
        });

        // map 实现，发射元组 (word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return Tuple2.of(value, 1);
            }
        });

        // keyBy 实现，按 word 分组并累加求和。
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy("f0").sum(1);

        */


        /* 写法（三），flatMap 与 map 组合实现。 */
        /*
        // flatMap 实现了 Map 的功能，直接发射元组 (word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socket.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String ss : s) {
                    out.collect(Tuple2.of(ss, 1));
                }
            }
        });

        // keyBy 实现，按 word 分组进行统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(f -> f.f0).sum(1);

         */


        /* 写法（四），RichFlatMap 实现，功能稍多一些。 */
        /*
        // 实现 RichFlatMapFunction
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socket.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            // 模拟 mysql 的连接。
            private String name = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                name = "tag: ";
            }

            @Override
            public void close() throws Exception {
                System.out.println("close.");
                name = null;
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

                String[] s = value.split(" ");

                for (String ss: s) {
                    out.collect(Tuple2.of(indexOfThisSubtask + "_" + name + ss, 1));
                }
            }
        });

        // key 的选择器写法。key 是 String 类型。
        KeyedStream<Tuple2<String, Integer>, String> map = flatMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.sum(1);

         */

        /* 写法（五），PrcessFunction 自己实现 flatMap 功能。 */
        // ProcessFunction 重写 onTimer() 方法，自己实现窗口功能。
/*        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socket.process(new ProcessFunction<String, Tuple2<String, Integer>>() {

            private String name = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                name = "tag: ";
            }

            @Override
            public void close() throws Exception {
                name = null;
            }


            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                // 3秒钟回调 onTimer() 方法。
                // ctx.timerService().registerEventTimeTimer(3000);

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());

                String[] s = value.split(" ");

                for (String ss : s) {
                    out.collect(Tuple2.of(name + ss, 1));
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });*/


        /* 多个 Process 实现。 */
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socket.process(new ProcessFunction<String, Tuple2<String, Integer>>() {

            private String name = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                name = "tag: ";
            }

            @Override
            public void close() throws Exception {
                name = null;
            }


            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                // 3秒钟回调 onTimer() 方法。
                // ctx.timerService().registerEventTimeTimer(3000);

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());

                String[] s = value.split(" ");

                for (String ss : s) {
                    out.collect(Tuple2.of(name + ss, 1));
                }
            }
        }).setParallelism(2).keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            // 注意：这个 num 只会初始化一次。但是，遗留问题是如果程序抛出异常的话，这个变量的值将丢失，将被垃圾回收销毁了。所以这块得加入记录状态的代码！
            private Integer num = 0;

            // 注意：同一样的可以会进入到相同的 KeyedProcessFunction() 里面去。
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                // 注意：ctx 中包含了当前 Process 中的 key 值。
                // Tuple currentKey = ctx.getCurrentKey();
                num += value.f1;

                out.collect(Tuple2.of(value.f0, num));
            }
        });

        // 输出打印
        sum.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return true;
            }
        }).setParallelism(2).disableChaining().print().setParallelism(2);

        System.out.println(env.getExecutionPlan());

        env.execute("java word count.");
    }
}
