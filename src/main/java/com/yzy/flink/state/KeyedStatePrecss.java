package com.yzy.flink.state;

import com.yzy.flink.source.HdfsFileSourceFunction;
import com.yzy.flink.source.MyKafkaRecord;
import com.yzy.flink.source.MyKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/30 16:10
 * @Description MapState 的使用。
 *
 */
public class KeyedStatePrecss {

    public static void main(String[] args) throws Exception {

        //--- env ---//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //--- checkpoint: 3 steps 激活---//
        // step-1：配置
        env.enableCheckpointing(1000); // 启用 checkpoint，执行间隔为1000(ms)。
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


        //--- source ---//
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.209.101:9092");
        properties.setProperty("group.id", "test");

        // Kafka DataSource 源。
        // 每条记录使用自定义的 MyKafkaRecord 格式对象。
        FlinkKafkaConsumer<MyKafkaRecord> kafkaSource = new FlinkKafkaConsumer<>("test", new MyKafkaRecordSchema(), properties);

        // kafkaSource.setStartFromLatest();
        DataStreamSource<MyKafkaRecord> kafkaInputSource = env.addSource(kafkaSource);
        // 对kafka的DataStream进行keyBy操作。
        KeyedStream<MyKafkaRecord, String> kafkaRecordStringKeyedStream = kafkaInputSource.keyBy(new KeySelector<MyKafkaRecord, String>() {
            @Override
            public String getKey(MyKafkaRecord value) throws Exception {
                return value.getRecord();
            }
        });


        // HDFS source
        DataStreamSource<String> countryDictSource = env.addSource(new FileSourceOperatorStateListCheckpointedFunction());
        // 做个 map 操作，发射元组
        KeyedStream<Tuple2<String, String>, String> HdfsRecordStringKeyedStream = countryDictSource.map(new MapFunction<String, Tuple2<String, String>>() {

            // String转换为Tuple类型。
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            // 第一个字段作为为Key。
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });


        //--- operator ---//
        // connect 处理逻辑。
        // （两个 keyedStream 数据流）connect 连接操作，即相同的key在一起处理。
        ConnectedStreams<Tuple2<String, String>, MyKafkaRecord> connect = HdfsRecordStringKeyedStream.connect(kafkaRecordStringKeyedStream);
        // 左边是hdfs（Tuple类型），右边是kafka（自定义的MyKafkaRecord类型）。
        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<String, Tuple2<String, String>, MyKafkaRecord, String>() {

            // 定义一个 MapState
            private MapState<String, String> ms = null;


            // open() 方法中恢复 MapState 中存储的数据。
            @Override
            public void open(Configuration parameters) throws Exception {

                // 1.一秒钟就超时；
                // 2.写入的时候就决定了初始时间；
                // 3.永不会返回；
                // 4.使用处理时间；
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(100))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                        .build();

                // 恢复 MapState 中的数据。
                MapStateDescriptor<String, String> msd = new MapStateDescriptor<>("map", String.class, String.class);
                msd.enableTimeToLive(ttlConfig); // 注意要设置 MapState 的 TTL 值，因为存储时没有被带入存储，这样恢复使用时会报错！
                ms = getRuntimeContext().getMapState(msd);
            }


            // 左边是 hdfs 数据流。
            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                ms.put(value.f0, value.f1);
                out.collect(value.toString()); // 发射是为了看看验证一下

            }

            // 右边是 kafka 数据流。
            @Override
            public void processElement2(MyKafkaRecord value, Context ctx, Collector<String> out) throws Exception {

                // 查看 MapState 中保存的数据。
                // 注意：只会打印本 subTask 的 state 的值。
                Iterable<Map.Entry<String, String>> entries = ms.entries();
                for (Map.Entry<String, String> entry : entries) {
                    System.out.println(entry.getKey());
                    System.out.println(entry.getValue());
                }

                // 这里故意设置一个异常后，验证 MapState 的恢复功能！
                if (value.getRecord().equals("Angola")) {
                    int i = 1 / 0;
                }


                // 两种方法：
                // String countryName = map.get(value.getRecord());
                String countryName = ms.get(ctx.getCurrentKey());

                String outStr = countryName == null ? "no match" : countryName;
                // 处理结果发射出去
                out.collect(outStr);

            }
        });


        //--- sink ---//
        process.print();


        //--- submit ---//
        env.execute("my job.");

    }
}
