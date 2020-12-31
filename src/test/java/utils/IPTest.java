package utils;

import com.yzy.flink.utils.IPUtil;
import org.junit.Test;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/30 11:34
 * @Description
 */
public class IPTest {

    @Test
    public void test1() {
        System.out.println(IPUtil.ip2long("192.168.209.101"));
    }
}
