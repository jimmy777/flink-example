package tests;

import beans.Course;
import beans.JsonFile;
import beans.Student;
import beans.Teacher;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 8:23
 * @Description fastjson 的处理。
 *
 * ref: https://segmentfault.com/a/1190000011212806
 *
 * 注意：
 * 1. json 与 jopo 互转；
 * 2. json 与 String 互转；
 *
 */
public class FastjsonTest {

    // json字符串-简单对象型
    private static final String  JSON_OBJ_STR = "{\"studentName\":\"lily\",\"studentAge\":12}";

    // json字符串-数组类型
    private static final String  JSON_ARRAY_STR = "[{\"studentName\":\"lily\",\"studentAge\":12},{\"studentName\":\"lucy\",\"studentAge\":15}]";

    // 复杂格式json字符串
    private static final String  COMPLEX_JSON_STR = "{\"teacherName\":\"crystall\",\"teacherAge\":27,\"course\":{\"courseName\":\"english\",\"code\":1270},\"students\":[{\"studentName\":\"lily\",\"studentAge\":12},{\"studentName\":\"lucy\",\"studentAge\":15}]}";

    // 保存 json 文件中的内容
    private String jsonf1;

    @Before
    public void setUp() throws Exception {
        // 读取 json 文件，转换成 String 类型保存。
        jsonf1 = JsonFile.readJsonFile("e:\\tmp\\student.json");
    }


    @Test
    public void test1() {
        // 解析 json 对象
        JSONObject jsonObject = JSONObject.parseObject(JSON_OBJ_STR);

        System.out.println(jsonObject.getString("studentName") +"\t"+jsonObject.getInteger("studentAge"));
    }


    /**
     * JSONObject到json字符串-简单对象型的转换
     */
    @Test
    public void test2() {
        // 解析 json 对象
        JSONObject jsonObject = JSONObject.parseObject(JSON_OBJ_STR);

        // 已知JSONObject,目标要转换为json字符串，并使用两种转换方法。
        System.out.println("方法一：" + JSONObject.toJSONString(jsonObject));
        System.out.println("方法二：" + jsonObject.toJSONString());
    }

    /**
     * json字符串-数组类型到JSONArray的转换
     */
    @Test
    public void test3() {
        // 获取 Array 类型
        JSONArray jsonArray = JSONArray.parseArray(JSON_ARRAY_STR);

        // 遍历方式1
        System.out.println("遍历方法一：");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);

            System.out.println(jsonObject.getString("studentName") +"\t"+jsonObject.getInteger("studentAge"));
        }

        // 遍历方式2
        System.out.println("遍历方法二：");
        for (Object obj :jsonArray) {
            JSONObject jsonObject = (JSONObject)obj;

            System.out.println(jsonObject.getString("studentName") +"\t"+jsonObject.getInteger("studentAge"));

        }
    }

    /**
     * 从 json 文件中读取数据
     */
    @Test
    public void test4() {
        // 解析 json 对象
        JSONObject jsonObject = JSONObject.parseObject(jsonf1);

        System.out.println(jsonObject.getString("studentName") +"\t"+jsonObject.getInteger("studentAge"));
    }

    @Test
    public void test5() {
        // 解析 json 对象
        JSONObject jsonObject = JSONObject.parseObject(COMPLEX_JSON_STR);
        System.out.println(jsonObject.getString("teacherName") +"\t"+jsonObject.getInteger("teacherAge"));

        // 获取 map 字段
        JSONObject course = jsonObject.getJSONObject("course");
        System.out.println(course.getString("courseName") + "\t" + course.getInteger("code"));


        // 获取 array 字段，并遍历 JSONArray
        JSONArray students = jsonObject.getJSONArray("students");
        for (Object o: students) {
            JSONObject student = (JSONObject)o;
            System.out.println(student.getString("studentName") +"\t"+student.getInteger("studentAge"));
        }
    }


    /**
     * json字符串-简单对象到JavaBean之间的转换
     */
    @Test
    public void test6() {
        // 解析 json 对象
        JSONObject jsonObject = JSONObject.parseObject(JSON_OBJ_STR);
        // 方法一
        Student student = new Student(jsonObject.getString("studentName"), jsonObject.getInteger("studentAge"));
        System.out.println(student);

        // 方法二：使用TypeReference<T>类,由于其构造方法使用protected进行修饰,故创建其子类
        Student student2 = JSONObject.parseObject(JSON_OBJ_STR, new TypeReference<Student>() {});
        System.out.println(student2);

        // 方法三：使用Gson的思想
        Student student1 = JSONObject.parseObject(JSON_OBJ_STR, Student.class);
        System.out.println(student1);
    }


    /**
     * JavaBean到json字符串-简单对象的转换
     */
    @Test
    public void test7() {
        Student tom = new Student("tom", 12);
        String s = JSONObject.toJSONString(tom);
        System.out.println(s);
    }

    /**
     * json字符串-数组类型到JavaBean_List的转换
     */
    @Test
    public void test8() {
        // 解析 json 对象
        JSONArray jsonArray = JSONArray.parseArray(JSON_ARRAY_STR);

        //遍历JSONArray
        ArrayList<Student> students = new ArrayList<>();
        Student student;
        for (Object o: jsonArray) {
            JSONObject jsonObject = (JSONObject)o;
            student = new Student(jsonObject.getString("studentName"), jsonObject.getInteger("studentAge"));
            students.add(student);
        }

        System.out.println("students: " + students);
    }

    /**
     * 复杂json格式字符串到JavaBean_obj的转换
     */
    @Test
    public void test9() {
        // 第一种方式,使用TypeReference<T>类,由于其构造方法使用protected进行修饰,故创建其子类
        Teacher teacher = JSONObject.parseObject(COMPLEX_JSON_STR, new TypeReference<Teacher>() {});
        System.out.println(teacher);

        // 第二种方式,使用Gson思想
        Teacher teacher1 = JSONObject.parseObject(COMPLEX_JSON_STR, Teacher.class);
        System.out.println(teacher1);
    }


    /**
     * 复杂JavaBean_obj到json格式字符串的转换
     */
    @Test
    public void test10() {
        Teacher teacher = JSONObject.parseObject(COMPLEX_JSON_STR, new TypeReference<Teacher>() {});
        String s = JSONObject.toJSONString(teacher);
        System.out.println(s);
    }


    @Test
    public void test11() {
        ArrayList<Student> students = new ArrayList<>();
        students.add(new Student("tom", 20));
        students.add(new Student("jack", 19));

        Course english = new Course("english", 1270);

        Teacher crystall = new Teacher("crystall", 27, english, students);

        String s = JSONObject.toJSONString(crystall);
        System.out.println(s);

        JSONObject jsonObject = JSONObject.parseObject(s);
        System.out.println(jsonObject);

        JSONObject jsonObject1 = (JSONObject)JSONObject.toJSON(crystall);
        System.out.println(jsonObject1);


    }


}
