package tests;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 9:12
 * @Description
 */
public class RegexTest {

    // 正则表达式: 验证手机号
    public static final String REGEX_MOBILE = "^((13[0-9])|(14[5,7])|(15[0-3,5-9])|(17[0,3,5-8])|(18[0-9])|166|198|199|(147))\\d{8}$";

    // 验证邮箱
    public static final String REGEX_EMAIL = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";

    // 验证一个汉字
    public static final String REGEX_CHINESE = "^[\u4e00-\u9fa5],{0,}$";

    // 验证身份证
    public static final String REGEX_ID_CARD = "(^\\d{18}$)|(^\\d{15}$)";

    // 验证URL
    public static final String REGEX_URL = "http(s)?://([\\w-]+\\.)+[\\w-]+(/[\\w- ./?%&=]*)?";

    // 验证IP地址
    public static final String REGEX_IP_ADDR = "(25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)";


    //--------------------- 开始 ---------------------//
    // 验证手机号
    @Test
    public void test1() {
        String mobile = "13911112222";
        boolean b = StringUtils.isNoneBlank(mobile) && Pattern.matches(REGEX_MOBILE, mobile);

        System.out.println(b);
    }


    // 验证邮箱
    @Test
    public void test2() {
        String email = "abc@126.com";
        boolean b = StringUtils.isNotBlank(email) && Pattern.matches(REGEX_EMAIL, email);

        System.out.println(b);
    }

    // 验证一个汉字
    @Test
    public void test3() {
        String chinese = "我";
        boolean b = StringUtils.isNotBlank(chinese) && Pattern.matches(REGEX_CHINESE, chinese);

        System.out.println(b);
    }

    // 验证身份证
    @Test
    public void test4() {
        String idCard = "210555200010101111";
        boolean b = StringUtils.isNotBlank(idCard) && Pattern.matches(REGEX_ID_CARD, idCard);

        System.out.println(b);
    }

    // 验证URL
    @Test
    public void test5() {
        String url = "https://www.cnblogs.com/";
        boolean b = StringUtils.isNotBlank(url) && Pattern.matches(REGEX_URL, url);

        System.out.println(b);
    }

    // 验证IP地址(这个有点问题！)
    @Test
    public void test6() {
        String ipAddr = "255.255.255.254";
        boolean b = StringUtils.isNotBlank(ipAddr) && Pattern.matches(REGEX_IP_ADDR, ipAddr);

        System.out.println(b);
    }
}
