package dataProcess.tool;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Created by sghipr on 11/04/16.
 * 学生基本信息记录.
 */
public class GraduateStudentBasicRecord implements Writable{
    protected String studentID;
    protected String nation;
    protected String gender;
    protected String political;
    protected String major;
    protected String work;
    protected String college;

    public String getStudentID(){
        return studentID;
    }

    public String getNation(){
        return nation;
    }

    public String getGender(){
        return gender;
    }
    public String getMajor(){
        return major;
    }

    public String getWork(){
        return  work;
    }
    public String getCollege(){
        return college;
    }

    public String getPolitical(){
        return political;
    }

    public GraduateStudentBasicRecord(){}
    public GraduateStudentBasicRecord(String studentID, String gender, String nation, String political, String college, String major, String work){
        this.studentID = studentID;
        this.gender = gender;
        this.nation = nation;
        this.political = political;
        this.college = college;
        this.major = major;
        this.work = work;
    }

    /**
     * 深度复制
     * @param o
     */
    public GraduateStudentBasicRecord(GraduateStudentBasicRecord o){
        this(o.studentID,o.gender,o.nation,o.political,o.college,o.major,o.work);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(studentID);
        out.writeUTF(gender);
        out.writeUTF(nation);
        out.writeUTF(political);
        out.writeUTF(college);
        out.writeUTF(major);
        out.writeUTF(work);
    }

    public void readFields(DataInput in) throws IOException {
        studentID = in.readUTF();
        gender = in.readUTF();
        nation = in.readUTF();
        political = in.readUTF();
        college = in.readUTF();
        major = in.readUTF();
        work = in.readUTF();
    }
}
