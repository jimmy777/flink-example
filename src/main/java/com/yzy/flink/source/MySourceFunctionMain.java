package com.yzy.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/23 9:54
 * @Description 创建一个Flink任务并提交。
 *
 * 1. 创建环境 env；
 * 2. 加入自定义的数据源source；
 * 3. 简单的sink操作为print打印输出；
 * 4. execute提交任务执行。
 *
 */
public class MySourceFunctionMain {

    public static void main(String[] args) throws Exception {

        // 创建一个本地环境env
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 添加自定义的source
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(new MySourceFunction());

        // 简单的sink操作，print操作
        stringDataStreamSource.print();

        // 提交执行。
        executionEnvironment.execute("test file source job.");
    }
}
