package beans;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.Date;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 8:37
 * @Description
 */
public class Person implements Serializable {

    @JSONField(name="age")
    private int age;

    @JSONField(name="fullname")
    private String fullName;

    @JSONField(name="birthdate")
    private Date dataOfBirth;

    public Person(int age, String fullName, Date dataOfBirth) {
        this.age = age;
        this.fullName = fullName;
        this.dataOfBirth = dataOfBirth;
    }

    @Override
    public String toString() {
        return "Person{" +
                "age=" + age +
                ", fullName='" + fullName + '\'' +
                ", dataOfBirth=" + dataOfBirth +
                '}';
    }
}
