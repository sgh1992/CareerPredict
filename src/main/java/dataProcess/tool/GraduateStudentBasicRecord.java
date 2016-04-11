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
