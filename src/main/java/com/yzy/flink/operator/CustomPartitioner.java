package com.yzy.flink.operator;

import com.yzy.flink.source.MyKafkaRecord;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/26 14:16
 * @Description
 */
public class CustomPartitioner implements Partitioner<MyKafkaRecord> {
    @Override
    public int partition(MyKafkaRecord key, int numPartitions) {
        if (key.getRecord().contains("Angola")) {
            return 1;
        } else {
            return 0;
        }
    }
}
