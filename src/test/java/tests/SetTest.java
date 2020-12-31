package tests;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 9:37
 * @Description
 */
public class SetTest {

    @Test
    public void test1(){

        ArrayList<String> list = new ArrayList<>();

        list.add("A");
        list.add("B");

        System.out.println(list);

        for (String s: list) {
            System.out.println(s);
        }

        System.out.println(list.size());
        System.out.println(list.indexOf("B"));
    }

    @Test
    public void test2(){

        ArrayList<Map<String, Object>> list = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("id", i);
            list.add(map);
        }

        System.out.println(list);

        HashMap<String, Object> map = new HashMap<>();
        map.put("id", 8);

        System.out.println(list.contains(map));
    }

}
