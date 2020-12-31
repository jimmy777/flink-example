package com.yzy.flink.file;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/24 15:43
 * @Description Flink按列存储记录，使用 parquet 格式。
 *
 * 注意：
 * 1. DateTimeBucketAssigner 生成文件目录，后续可以使用 hive 或者 presto 加载访问。
 * 2. 利用 checkpoint 成功了才生成文件，不然二进制文件容易被截断而不可用。
 *
 */
public class Streaming2ColumnFormatFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //--- 以下三步完成 checkpoint 的使用。
        // 第一步：开启 checkpoint
        // 1秒钟 checkpoint 一次
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 保存 EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 每次 ck 之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 每次 ck 的超时时间
        checkpointConfig.setCheckpointTimeout(20000L);
        // 如果 ck 执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        // job 在执行 CANEL 的时候是否删除 ck 数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 第二步：指定保存 ck 的存储模式，这个是默认的。
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(10 * 1024 * 1024, false);
        env.setStateBackend(memoryStateBackend);

        // 第三步：恢复策略
        env.setRestartStrategy(
                // 重试3次，延迟0秒。
                RestartStrategies.fixedDelayRestart(3, Time.of(0, TimeUnit.SECONDS))
        );

        //--- checkpoint 配置结束。


        // socket 源
        DataStreamSource<String> socket = env.socketTextStream("192.168.209.101", 6666);

        // map 映射为 pojo 对象
        SingleOutputStreamOperator<ParquetPojo> map = socket.map(f -> Tuple2.of(f, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0).sum(1).map(f -> new ParquetPojo(f.f0, f.f1));

        // 生成的文件路径
        DateTimeBucketAssigner<ParquetPojo> bucketAssigner = new DateTimeBucketAssigner<>("yyyy/MMdd/HH", ZoneId.of("Asia/Shanghai"));

        // 将 pojo 对象写入 parquet 文件
        // 可以分隔文件保存
        StreamingFileSink<ParquetPojo> parquetSink = StreamingFileSink.forBulkFormat(new Path("file:///e:/tmp/flink/out/columnformat"), ParquetAvroWriters.forReflectRecord(ParquetPojo.class))
                .withBucketAssigner(bucketAssigner).build();


        map.addSink(parquetSink);

        env.execute("my job.");
    }
}
