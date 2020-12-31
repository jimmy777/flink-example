package beans;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 9:56
 * @Description
 */
public class Student {

    private String studentName;
    private Integer studentAge;

    public Student(String studentName, Integer studentAge) {
        this.studentName = studentName;
        this.studentAge = studentAge;
    }

    public String getStudentName() {
        return studentName;
    }


    public Integer getStudentAge() {
        return studentAge;
    }


    @Override
    public String toString() {
        return "Student{" +
                "studentName='" + studentName + '\'' +
                ", studentAge=" + studentAge +
                '}';
    }
}
