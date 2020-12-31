package com.yzy.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 10:43
 * @Description
 */
public class WindowsAllOperator {

    public static void main(String[] args) throws Exception {

        // 创建本地环境env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 配置source源为socket
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666, "\n");

        // transaction: flatMap -> map -> keyBy
        // flatMap
        SingleOutputStreamOperator<String> flatMap = socket.flatMap((String value, Collector<String> out) -> {
            for (String s : value.split(",")) {
                out.collect(s);
            }

        }).returns(Types.STRING);

        // map
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        /* keyBy
         Tuple2<String, Integer> 是进行keyBy的数据类型
         String                  是分流的key的数据类型
         */
        // KeyedStream<Tuple2<String, Integer>, String> tuple2TupleKeyedStream = map.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = map.keyBy(f -> f.f0);
        
        // windowAll
        
        tuple2StringKeyedStream.timeWindowAll(Time.seconds(2)).process(new ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                for (Iterator<Tuple2<String, Integer>> iterator = elements.iterator(); iterator.hasNext();) {
                    Tuple2<String, Integer> next = iterator.next();
                    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                    System.out.println(indexOfThisSubtask + ": " + next);
                }
            }
        });

        // sink
        tuple2StringKeyedStream.print();

        // submit
        env.execute("test flink job.");
    }
}
