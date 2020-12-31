package com.yzy.flink.operator;



import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.HdfsFileSourceMapFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/25 15:35
 * @Description 两个流的 connect 操作。
 *
 * 注意：
 * 1. 小表（HDFS 文件）进行广播处理，即所有的 subTask 都能有有一份数据（map 数据）。
 * 2. 产生 BroadcastStream 广播数据流。
 * 3. 广播的数据存入一个 MapState 中保存，防止丢失。
 *
 *
 */
public class ConnectOperatorMap {

    // MapStateDescriptor 的 MapState 的描述，共用了这个描述对象。
    private static MapStateDescriptor<String, String> broadcast1 = new MapStateDescriptor<>("broadcast", String.class, String.class);

    public static void main(String[] args) throws Exception {

        //--- env
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

        //--- source: kafka
        // 1.配置一些 kafka 服务的属性。
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.209.101:9092");
        properties.setProperty("group.id", "test");
        // 2.kafka comsumer 消费者。
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);
        // kafkaSource.setStartFromLatest();
        // 3.把 kafka 加入 Flink source 源。
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);


        //--- source: hdfs
        DataStreamSource<Map<String, String>> countryDictSource = env.addSource(new HdfsFileSourceMapFunction());
        BroadcastStream<Map<String, String>> broadcast = countryDictSource.broadcast(broadcast1); // 广播出去，生产广播流。

        //--- operator: connect 操作
        BroadcastConnectedStream<MyKafkaRecord, Map<String, String>> connect = kafkaInputSource.connect(broadcast);
        // connect 后的 process 处理，数据处理逻辑。
        SingleOutputStreamOperator<String> process = connect.process(new BroadcastProcessFunction<MyKafkaRecord, Map<String, String>, String>() {

            // 流1：kafka 的。
            @Override
            public void processElement(MyKafkaRecord value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcast1);

                String countryCode = value.getRecord();
                String countryName = broadcastState.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;
                // 处理结果发射出去
                out.collect(outStr);

            }

            // 流2：广播 hdfs 的。
            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcast1);

                Set<Map.Entry<String, String>> entries = value.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    broadcastState.put(entry.getKey(), entry.getValue());
                }

                out.collect(value.toString());
            }
        });


        //--- sink
        process.print();


        //--- submit
        env.execute("my job.");
    }
}
