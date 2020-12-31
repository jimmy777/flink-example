package tests;

import org.junit.Test;

import java.text.MessageFormat;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 8:57
 * @Description
 */
public class StringBuilderTest {

    StringBuilder sb = new StringBuilder();

    @Test
    public void test1() {
        sb.append("SELECT ''{0}'' FROM ''{1}''");

        Object[] strings = {"name", "student"};

        String format = MessageFormat.format(sb.toString(), strings);
        System.out.println(format);
    }

    @Test
    public void test3(){

        StringBuffer stringBuffer = new StringBuffer();

        stringBuffer.append("ABC");
        stringBuffer.append("@");
        stringBuffer.append("126");
        stringBuffer.append(".");
        stringBuffer.append("com");

        System.out.println(stringBuffer.toString());
    }
}
