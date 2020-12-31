package tests;

import org.elasticsearch.common.collect.Maps;
import org.junit.Test;

import java.util.HashMap;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/8 15:01
 * @Description
 */
public class HashMapTest {

    @Test
    public void test1() {
        HashMap<String, String> config = Maps.newHashMap();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "cluster1");

        System.out.println(config);

    }
}
