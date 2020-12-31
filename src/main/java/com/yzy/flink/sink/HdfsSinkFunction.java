package com.yzy.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 15:55
 * @Description RichSinkFunction 使用。
 *
 * 注意：
 * 1、sink function 没有并行度之分，其取决于上游 operator 的并行度。
 * 2、RichSinkFunction 没有 cancal() 方法，因为 sink 不负责发数据、只负责写数据。
 * 3、invoke() 方法负责具体写数据。
 * 4、上游来一条数据就会调用一次 invoke() 方法，所以不用自己写循环调用。（这与 SourceFunction 不同！）
 * 5、hdfs 的 append 模式无法压缩处理。
 *
 */
public class HdfsSinkFunction extends RichSinkFunction<String> {

    private FileSystem fs = null;
    private String pathStr = null;
    private SimpleDateFormat sf = null;

    @Override
    public void open(Configuration parameters) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        fs = FileSystem.get(hadoopConf);

        sf = new SimpleDateFormat("yyyyMMddHH");

        pathStr = "hdfs://192.168.209.101:9000/out/flink/output";

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if( null != value){
            // 获取日期字符串
            String format = sf.format(new Date());

            // 获得分区id
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();


            // 拼接字符串为 hdfs 上的保存文件的路径。
            StringBuilder sb = new StringBuilder();
            sb.append(pathStr).append("/").append(indexOfThisSubtask).append("_").append(format);
            System.out.println(sb.toString());
            Path path = new Path(sb.toString());

            // 判断 hdfs 上保存文件的路径是否存在，如果存在就是 append 模式，不存在就是 create 模式。
            FSDataOutputStream fsd = null;
            if (fs.exists(path)){
                fsd = fs.append(path);
            } else {
                fsd = fs.create(path);
            }

            // 写数据，字符串。注意编码方式。
            fsd.write((value + "\n").getBytes(StandardCharsets.UTF_8));

            // 关闭out流。
            fsd.close();

        }
    }
}
