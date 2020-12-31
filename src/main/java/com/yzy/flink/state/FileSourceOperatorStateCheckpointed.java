package com.yzy.flink.state;

import com.yzy.flink.source.HdfsFileSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.tokens.FlowSequenceStartToken;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/1 13:57
 * @Description 开启 checkpoint 机制。
 *
 * 注意：
 * 1. env 中设置 checkpoint 开启及其配置。
 * 2. checkpoint 为插入了一条  barrier 数据栏栅。即 source 端发出的数据加入了一个标识，后面的 operator 根据这个标识来触发 checkpoint 将状态保存到 state 中。这个过程直到栏栅数据经过 sink 说明数据已经成功处理完毕。
 * 3. 三种保存 state 值的地方：默认内存、本地文件、hdfs文件。
 *
 */
public class FileSourceOperatorStateCheckpointed {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");

        //--- env ---//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()); // 开发模式下
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 打包提交生产状态下。

        // --- checkpoint: 3 step
        // step-1：配置 checkpoint。
        // 即：周期性调用 snapshotState() 函数去保存状态数据。
        env.enableCheckpointing(1000); // 启用 checkpoint，执行间隔为1000(ms)。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 只处理一次模式。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 两次 checkpoint 间隔时间(ms)。
        env.getCheckpointConfig().setCheckpointTimeout(60000); // checkpoint 超时(ms)。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // checkpoint 并发度为1。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 任务失败后 checkpoint 记录仍然保存给外部存储。
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);  // checkpoint 执行失败或者错误时，任务是否关闭。
        // step-2：checkpoint 怎么恢复。
        // 即：调用 initializeState() 函数去初始化恢复状态数据。
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3,
                        Time.of(0, TimeUnit.SECONDS)
                )
        );
        // step-3：三种方式来保存。（默认内存、本地文件、hdfs文件。）

        // FsStateBackend fsStateBackend = new FsStateBackend("file:///E:\\tmp\\flink\\checkpoints"); // 1.保存在本地文件中。
        // FsStateBackend fsStateBackend = new FsStateBackend("hdfs://192.168.209.101:9000/flink/checkpoints"); // 2.保存在 hdfs 文件中。
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(10 * 1024 * 1024, false); // 3. 保存在内存中。
        env.setStateBackend(memoryStateBackend);


        // --- source ---//
        DataStreamSource<String> stringDataStreamSource = env.addSource(new FileSourceOperatorStateCheckpointedFunction());

        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                if (value.contains("Angola")) {
                    int a = 1 / 0;
                }
                return value;
            }
        }).print().setParallelism(4); // --- sink ---//


        //--- submit ---//
        env.execute("my job.");
    }
}
