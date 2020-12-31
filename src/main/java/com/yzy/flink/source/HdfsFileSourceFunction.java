package com.yzy.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/23 9:50
 * @Description 自己写Source，实现SourceFunction。从写run方法。
 *
 * 注意：
 * 1、SourceFunction 为非并行方法。如读文件、读Socket。
 * 2、注意：source 退出的话，后面的操作都会退出。利用控制变量，自己控制退出。
 * 3、run方法中，循环处理source发过来的数据，并collect发射数据出去。利用循环变量来控制循环结束。
 *
 */
public class HdfsFileSourceFunction implements SourceFunction<String> {
    // 循环控制变量，控制 source 程序退出。
    private Boolean isCancel = true;

    // 利用 md5 值来判断文件是否变化了。
    String md5 = null;

    @Override
    public void run(SourceContext ctx) throws Exception {
        Path path = new Path("hdfs://192.168.209.101:9000/in/CountryDict.txt");
        FileSystem fs = FileSystem.get(new Configuration());

        // 不停地（每隔10秒一次）判断文件md5值是否变化。
        while (isCancel){

            if (!fs.exists(path)) {
                Thread.sleep(10000);
                continue;
            }

            FileChecksum fileChecksum = fs.getFileChecksum(path);
            String md5Str = fileChecksum.toString();
            String currentStr = md5Str.substring(md5Str.indexOf(":") +1);

            if (!currentStr.equals(md5)){
                FSDataInputStream open = fs.open(path);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));

                String line = bufferedReader.readLine();

                while (line != null){
                    ctx.collect(line);

                    line = bufferedReader.readLine();
                }
                bufferedReader.close();
                md5 = currentStr;
            }

            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        System.out.println("hello, Cancel join.");
        isCancel = false;
    }
}
