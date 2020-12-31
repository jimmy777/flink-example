package com.yzy.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/30 14:36
 * @Description ArrayList source 源。
 *
 *
 */
public class ArrayListSource {

    public static void main(String[] args) throws Exception {

        //-------------------- env --------------------//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //-------------------- source --------------------//
        ArrayList arrayList = new ArrayList();
        arrayList.add("a");
        arrayList.add("b");
        arrayList.add("c");
        arrayList.add("d");

        DataStreamSource dataStreamSource = env.fromCollection(arrayList);


        //-------------------- sink --------------------//
        dataStreamSource.print().setParallelism(4);

        //-------------------- submit --------------------//
        env.execute("my job.");
    }
}
