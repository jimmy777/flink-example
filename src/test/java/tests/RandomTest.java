package tests;

import org.junit.Test;

import java.util.Random;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 8:36
 * @Description
 */
public class RandomTest {

    private Random random = new Random();

    @Test
    public void test1() {

        int i = random.nextInt(10);
        System.out.println("result=" + i);
    }

    // 生成[0,1.0)区间的小数
    @Test
    public void test2() {

        double v = random.nextDouble();
        System.out.println("result=" + v);
    }

    // 生成[0,10)区间的整数
    @Test
    public void test3() {
        int abs = Math.abs(random.nextInt() % 10);

        System.out.println("result=" + abs);
    }


}
