package com.yzy.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
 * @Description CheckpointedFunction 接口，实现 checkpoint 保存 operator state 的值。
 * <p>
 * 注意：
 * 1. 定义 ListState 变量来保存 operator 的 state 状态值；
 * 2. 实现 CheckpointedFunction 接口，实现 initializeState() 和 snapshotState() 方法来保存和恢复状态值；
 */
public class FileSourceOperatorStateCheckpointedFunction implements SourceFunction<String>, CheckpointedFunction {
    // 循环控制变量，控制 source 程序退出。
    private Boolean isCancel = true;

    // 利用 md5 值来判断文件是否变化了。
    private String md5 = null; // 原始状态。即：checkpoint 需要保存的原始数据。


    // checkpoint 需要的 state 变量。
    // operator 要使用 ListState 变量来保存。
    private ListState<String> ls = null; // 托管状态。


    @Override
    public void run(SourceContext ctx) throws Exception {
        Path path = new Path("hdfs://192.168.209.101:9000/in/CountryDict.txt");
        FileSystem fs = FileSystem.get(new Configuration());

        // 不停地（每隔10秒一次）判断文件md5值是否变化。
        while (isCancel) {

            if (!fs.exists(path)) {
                Thread.sleep(10000);
                continue;
            }
            System.out.println("md5 = " + md5);
            FileChecksum fileChecksum = fs.getFileChecksum(path);
            String md5Str = fileChecksum.toString();
            String currentStr = md5Str.substring(md5Str.indexOf(":") + 1);

            if (!currentStr.equals(md5)) {
                FSDataInputStream open = fs.open(path);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));

                String line = bufferedReader.readLine();

                while (line != null) {
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


    // 周期性的、保存 -> 状态数据，即：原始状态 -> 托管状态。
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        ls.clear(); // 先把list里面的状态列表值情况。
        ls.add(md5); // 再加入新值。
        System.out.println("hello!snapshut state.");
    }

    // 失败的时候、恢复（或者第一次初始化）-> 状态数据，即：托管状态 -> 原始状态。
    // context.isRestored() 判断两种状态：一是第一次运行的初始化，二是用恢复值来初始化。
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> lsd = new ListStateDescriptor<>("laoda", String.class);

        ls = context.getOperatorStateStore().getListState(lsd);

        // 判断是否用恢复值来初始化。
        if (context.isRestored()) {
            Iterable<String> strings = ls.get();
            String next = strings.iterator().next();

            md5 = next;
        }

    }
}
