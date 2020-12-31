package com.yzy.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/1 13:57
 * @Description
 */
public class FileSourceOperatorStateListCheckpointed {

    public static void main(String[] args) throws Exception {

        //--- env ---//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

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
        // step-3：存哪里

        // --- source ---//
        DataStreamSource<String> stringDataStreamSource = env.addSource(new FileSourceOperatorStateListCheckpointedFunction());

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
