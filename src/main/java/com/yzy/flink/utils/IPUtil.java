package com.yzy.flink.utils;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/30 11:30
 * @Description
 */
public class IPUtil {

    public static long ip2long(String ip) {

        String[] split = ip.split("\\.");
        if (split.length != 4) {
            return 0L;
        }

        long r = Long.parseLong(split[0]) << 24;
        r |= Long.parseLong(split[1]) << 16;
        r |= Long.parseLong(split[2]) << 8;
        r |= Long.parseLong(split[3]);

        return r;
    }
}
