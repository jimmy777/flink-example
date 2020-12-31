package com.yzy.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author Y.Z.Y
 * @date 2020/11/23 15:59
 * @description None.
 */
public class MyKafkaRecordSchema implements DeserializationSchema<MyKafkaRecord> {


    @Override
    public MyKafkaRecord deserialize(byte[] message) throws IOException {

        // 把kafka中的message转化为String类型后，初始化自定义的类。
        MyKafkaRecord myKafkaRecord = new MyKafkaRecord(new String(message));
        return myKafkaRecord;
    }

    /**
     *  有界流，还是无界流。
     */
    @Override
    public boolean isEndOfStream(MyKafkaRecord nextElement) {
        return false;
    }

    // 告诉框架，解析二进制的类型。
    @Override
    public TypeInformation<MyKafkaRecord> getProducedType() {
        return TypeInformation.of(MyKafkaRecord.class);
    }
}
