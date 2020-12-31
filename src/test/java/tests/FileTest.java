package tests;

import beans.JsonFile;
import beans.Person;
import org.junit.Test;
import utils.FileTools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Y.Z.Y
 * @Date 2020/11/27 9:35
 * @Description 文件的一些操作。
 *
 */
public class FileTest {

    private static final String FILENAME = "e:\\tmp\\persons.json";
    private File file;

    /**
     * 读 json 文件
     */
    @Test
    public void test1(){
        String s = JsonFile.readJsonFile("");
        System.out.println(s);

    }

    /**
     * 读文件方法（一）
     */
    @Test
    public void test2() {
        FileTools.readFileByBytes(FILENAME);
    }

    /**
     * 读文件方法（二）
     */
    @Test
    public void test3() {
        FileTools.readFileByChars(FILENAME);
    }


    /**
     * 读文件方法（三）
     */
    @Test
    public void test4() {
        FileTools.readFileByLines(FILENAME);
    }


    /**
     * 读文件方法（四）
     */
    @Test
    public void test5() {
        FileTools.readFileByRandomAccess(FILENAME);
    }

    /**
     * 写文件方法一：文件尾部追加内容。
     */
    @Test
    public void test6() {
        FileTools.appendMethod1(FILENAME, "1607483573,9.5,1\n");
    }

    /**
     * 写文件方法二：文件尾部追加内容。
     */
    @Test
    public void test7() {
        FileTools.appendMethod2(FILENAME, "1607483573,10.2,1\n");
    }


    /**
     * java 支持对对象的读写操作，所操作的对象必须实现Serializable接口。
     *
     */
    @Test
    public void test8() {

        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("e:\\tmp\\oos.dat")));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Person p1 = new Person(20, "tom", simpleDateFormat.parse("2020-10-10"));
            Person p2 = new Person(19, "jack", simpleDateFormat.parse("2020-9-1"));

            oos.writeObject(p1);
            oos.writeObject(p2);
            oos.flush();
            oos.close();

            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File("e:\\tmp\\oos.dat")));
            Person obj1 = (Person)ois.readObject();
            System.out.println(obj1.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
