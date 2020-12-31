package com.yzy.flink.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;


/**
 * @author Y.Z.Y
 * @date 2020/11/23 9:58
 * @description None.
 */
public class HdfsAppendContent {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration configuration = new Configuration();
        configuration.setBoolean("dfs.support.append", true);

        FileSystem fs;

        final String hdfsPath = "hdfs://192.168.209.101:9000/in/in.dat";
        final String inPath = "e:\\tmp\\new.txt";

        try {
            fs = FileSystem.get(URI.create(hdfsPath), configuration);

            BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(inPath));

            FSDataOutputStream outFile = fs.append(new Path(hdfsPath));

            IOUtils.copyBytes(bufIn, outFile, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
