package com.yzy.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 16:28
 * @Description socket -> hdfs sink 例子。
 *
 * 注意：
 * 1、调用自定义的 HDFS SinkFunction 实现 sink 写入文件。
 *
 */
public class HdfsSink {

    public static void main(String[] args) throws Exception {

        //-------------------- env --------------------//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //-------------------- source --------------------//
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666);


        //-------------------- sink --------------------//
        socket.addSink(new HdfsSinkFunction());


        //-------------------- submit --------------------//
        env.execute("my job.");
    }
}
