package com.yzy.flink.state;

import akka.actor.ChildRestartStats;
import com.yzy.flink.source.HdfsFileSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/30 11:30
 * @Description
 */
public class FileSourceRestart {

    public static void main(String[] args) throws Exception {

        //-------------------- env --------------------//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 增加3条命。
                Time.of(0, TimeUnit.SECONDS) // 实例死了马上就活。
        ));


        //-------------------- source --------------------//
        DataStreamSource<String> stringDataStreamSource = env.addSource(new HdfsFileSourceFunction());
        SingleOutputStreamOperator<String> map = stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                if (value.contains("Angola")) {
                    int a = 1 / 0;
                }
                return value;
            }
        });

        //-------------------- sink --------------------//
        map.print().setParallelism(4);

        //-------------------- submit --------------------//
        env.execute("my job.");
    }
}
