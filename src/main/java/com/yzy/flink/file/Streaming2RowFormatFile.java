package com.yzy.flink.file;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/24 14:35
 * @Description Flink 写行存记录的例子。
 *
 * 注意：
 * 1. 分桶去存文件。
 * 2. 按时间切割文件或者按时间来切割文件。
 * 3. 设置文件名的前缀、后缀和处理中的文件前缀等。
 *
 */
public class Streaming2RowFormatFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2个并行度，即有2个 subtask 同时写入文件。
        env.setParallelism(2);

        DataStreamSource<String> input = env.socketTextStream("192.168.209.101", 6666);

        BucketingSink<String> hdfsSink = new BucketingSink<>("file:///e:/tmp/flink/out/rowformat");

        hdfsSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd-HH", ZoneId.of("Asia/Shanghai")));
        // 按时间切割，如：每10秒切一次
        hdfsSink.setBatchRolloverInterval(10000);
        // 按文件大小切割，如：每10M切一次
        hdfsSink.setBatchSize(1024*1024*1);
        //
        hdfsSink.setPendingPrefix("CN");
        hdfsSink.setPendingSuffix(".txt");
        hdfsSink.setInProgressPrefix(".");
        //

        input.addSink(hdfsSink);

        env.execute("my job.");

    }
}
