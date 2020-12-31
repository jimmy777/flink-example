package com.yzy.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 11:19
 * @Description kafka connector.
 *
 * 过程 3 步：
 * 1. 配置 kafka 参数；
 * 2. 创建 FlinkKafkaConsumer 对象；
 * 3. kafka 加入到 flink source 中，生产 DataStream 对象。
 *
 * 注意：
 *   kafka 也可以作为 sink，将结果数据写入 topic 中。
 */

public class KafkaSourceState {

    public static void main(String[] args) throws Exception {

        //--- env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 读 kafka 的 3 个分区
        env.setParallelism(3);

        // 开启 checkpoint： 3 steps
        // step-1：配置
        env.enableCheckpointing(5000); // 启用 checkpoint，执行间隔为1000(ms)。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 只处理一次模式。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 两次 checkpoint 间隔时间(ms)。
        env.getCheckpointConfig().setCheckpointTimeout(60000); // checkpoint 超时(ms)。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // checkpoint 并发度为1。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 任务失败后 checkpoint 记录仍然保存给外部存储。
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);  // checkpoint 执行失败或者错误时，任务是否关闭。
        // step-2：怎么存
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3,
                        Time.of(0, TimeUnit.SECONDS)
                )
        );
        // step-3：存哪里
        FsStateBackend fsStateBackend = new FsStateBackend("file:///E:\\tmp\\flink\\checkpoints"); // 1.保存在本地文件中。
        env.setStateBackend(fsStateBackend);


        //--- kafka source
        // 配置 kafka 属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");
        // 每30秒检查发现kafka的topic分区是否有变化，这样Flink自适应发现分区变化。
        // properties.setProperty("flink.partition-discovery.interval-millis", "30000");
        /*
        If you get data in a different format, then you can also specify your custom schema for deserialization. By default, Flink supports string and JSON deserializers.
        这里直接使用内置的 SimpleStringSchema 字符串解析类来处理 String 类型。直接使用内置的 SimpleStringSchema 字符串解析类来处理 String 类型。
         */
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        kafkaSource.setStartFromGroupOffsets();
        // kafka 加入 flink 源
        DataStreamSource<String> kafka = env.addSource(kafkaSource);


        //--- sink
        kafka.print();


        //--- submit
        env.execute("my job.");
    }
}
