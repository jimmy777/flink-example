package com.yzy.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/7 15:13
 * @Description
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {

        //--- env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // ProcessingTime
        // IngestionTime
        // EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 配置source源为socket
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666, "\n");

        socket.uid("step1").name("显示 socket 数据");


        //--- sinka
        socket.print().setParallelism(4);

        //--- submit
        env.execute("my job.");
    }
}
