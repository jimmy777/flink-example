package tests;


import org.junit.Test;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class StringTest {

    @Test
    public void testMD5 () {
        String s = "md5:abcdefasdfasdfwerqfsasdfqwer";
        String md5 = s.substring(s.indexOf(":") + 1);

        System.out.println(md5);
    }

    @Test
    public void test2() {
        String s = "Antigua and Barbuda|安提瓜和巴布达|AG|1268|-12|";

        String[] split = s.split("|");

        System.out.println(split[0] + ":" + split[1]);

    }

    @Test
    public void test3() {

        Map<String, String> map1 = new HashMap<>();

        map1.put("001", "tom");
        map1.put("002", "jack");

        System.out.println(map1.toString());

    }

    @Test
    public void test4() {
        int i = new Random().nextInt(10) + 30;
        System.out.println(i);
    }

    @Test
    public void test5() {

        long currentTimeMillis = System.currentTimeMillis();
        int i = new Random().nextInt(10) + 30;
        String msg = currentTimeMillis + "," + i + ",001";
        System.out.println(msg);

        String[] split = msg.split(",");
        System.out.println(split[0]);
        System.out.println(msg.substring(14));
    }

}
