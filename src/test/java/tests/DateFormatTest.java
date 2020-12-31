package tests;

import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 8:46
 * @Description
 */
public class DateFormatTest {

    // Fri Nov 27 08:51:22 CST 2020
    @Test
    public void test1() {
        Date date = new Date();
        System.out.println(date);

        // 1606438555456
        long timeInMillis = Calendar.getInstance().getTimeInMillis();
        System.out.println(timeInMillis);
    }

    // 2020-11-27
    @Test
    public void test2() {
        String format = DateFormatUtils.ISO_DATE_FORMAT.format(new Date());

        System.out.println("Date=" + format);
    }

    // 2020-11-27+08:00
    @Test
    public void test3() {
        String format = DateFormatUtils.ISO_DATE_TIME_ZONE_FORMAT.format(new Date());

        System.out.println("Date=" + format);
    }

    // 2020-11-27 08:53:14
    @Test
    public void test4() {
        String format = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");

        System.out.println("Date=" + format);
    }

    // 2020-11-27 08:55:01
    @Test
    public void test5() {
        String format = DateFormatUtils.format(Calendar.getInstance().getTimeInMillis(), "yyyy-MM-dd HH:mm:ss");

        System.out.println("Date=" + format);
    }

    // 产生13位的时间戳，即毫秒级，如：1607499787469
    @Test
    public void test6() {
        long currentTimeMillis = System.currentTimeMillis();

        System.out.println("Timestamp=" + currentTimeMillis);
    }
}
