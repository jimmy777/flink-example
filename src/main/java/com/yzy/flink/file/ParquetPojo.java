package com.yzy.flink.file;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/24 15:40
 * @Description
 */
public class ParquetPojo {

    private String word;
    private Long count;

    public ParquetPojo(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
