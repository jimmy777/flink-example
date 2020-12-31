package com.yzy.flink.source;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/23 15:57
 * @Description
 *
 */
public class MyKafkaRecord {

    // 直接保存记录，类型为 String。
    private String record;

    public MyKafkaRecord(String record) {
        this.record = record;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime + result + ((record == null)?0:record.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "MyKafkaRecord{" +
                "record='" + record + '\'' +
                '}';
    }
}
