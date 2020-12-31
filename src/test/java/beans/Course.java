package beans;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/10 10:19
 * @Description
 */
public class Course {

    private String courseName;
    private Integer code;

    public Course(String courseName, Integer code) {
        this.courseName = courseName;
        this.code = code;
    }

    public String getCourseName() {
        return courseName;
    }

    public Integer getCode() {
        return code;
    }

    @Override
    public String toString() {
        return "Course{" +
                "courseName='" + courseName + '\'' +
                ", code=" + code +
                '}';
    }
}
