package com.yzy.flink.operator;

import com.yzy.flink.source.MyKafkaRecord;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/26 14:16
 * @Description
 */
public class CustomRandomPrefixPartitioner implements Partitioner<MyKafkaRecord> {
    @Override
    public int partition(MyKafkaRecord key, int numPartitions) {

        String[] s = key.getRecord().split("_");

        String randomId = s[0];

        return new Integer(randomId);
    }
}
