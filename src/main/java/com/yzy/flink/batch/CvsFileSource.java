package com.yzy.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/14 10:05
 * @Description
 */
public class CvsFileSource {

    public static void main(String[] args) throws Exception {

        //--- env
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<Integer, String, String>> cvsInput = env.readCsvFile("e:\\tmp\\mobile.csv").types(Integer.class, String.class, String.class);

        cvsInput.map(new MapFunction<Tuple3<Integer, String, String>, String>() {
            @Override
            public String map(Tuple3<Integer, String, String> value) throws Exception {
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        }).print();


        //--- submit
//        env.execute("my job.");
    }
}
