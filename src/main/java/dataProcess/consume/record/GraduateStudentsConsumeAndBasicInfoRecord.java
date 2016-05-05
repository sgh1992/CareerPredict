package dataProcess.consume.record;

import dataProcess.tool.GraduateStudentBasicRecord;
import dataProcess.tool.PlaceRecord;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 11/04/16.
 */
public class GraduateStudentsConsumeAndBasicInfoRecord implements Writable{

    protected String studentID;
    protected String nation;
    protected String gender;
    protected String political;
    protected String major;
    protected String work;
    protected String college;

    protected String type;
    protected String transferPlace;
    protected String kind;
    protected String position;
    protected String deviceID;
    protected String time;
    protected double amount;
    protected double balance;

    public String getTime(){
        return time;
    }

    public String getStudentID(){
        return studentID;
    }

    public String getKind(){
        return kind;
    }

    public double getAmount(){
        return amount;
    }

    public GraduateStudentsConsumeAndBasicInfoRecord(){}

    public GraduateStudentsConsumeAndBasicInfoRecord(String studentID,String nation,String gender,
                                                     String political,String college,String major,
                                                     String type,String transferPlace, String kind, String position,
                                                     String deviceID,String time,
                                                     double amount,double balance,String work){
        this.studentID = studentID;
        this.nation = nation;
        this.gender = gender;
        this.political = political;
        this.college =college;
        this.major = major;
        this.work = work;

        this.type = type;
        this.transferPlace = transferPlace;
        this.kind = kind;
        this.position = position;
        this.deviceID = deviceID;
        this.time = time;
        this.amount = amount;
        this.balance = balance;
    }

    public GraduateStudentsConsumeAndBasicInfoRecord(Record consume, GraduateStudentBasicRecord basicRecord, PlaceRecord place){

        this.studentID = basicRecord.getStudentID();
        this.nation = basicRecord.getNation();
        this.gender = basicRecord.getGender();
        this.political = basicRecord.getPolitical();
        this.college = basicRecord.getCollege();
        this.major = basicRecord.getMajor();
        this.work = basicRecord.getWork();

        this.type = consume.type;
        this.transferPlace = place.getTransferPlace();
        this.kind = place.getKind();
        this.position = place.getPosition();

        this.deviceID = consume.deviceID;
        this.time = consume.time;
        this.amount = consume.amount;
        this.balance = consume.balance;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(studentID);
        out.writeUTF(nation);
        out.writeUTF(gender);
        out.writeUTF(political);
        out.writeUTF(college);
        out.writeUTF(major);

        out.writeUTF(type);
        out.writeUTF(transferPlace);
        out.writeUTF(kind);
        out.writeUTF(position);
        out.writeUTF(deviceID);
        out.writeUTF(time);
        out.writeDouble(amount);
        out.writeDouble(balance);

        out.writeUTF(work);
    }

    public void readFields(DataInput in) throws IOException {

        this.studentID = in.readUTF();
        this.nation = in.readUTF();
        this.gender = in.readUTF();
        this.political = in.readUTF();
        this.college = in.readUTF();
        this.major = in.readUTF();

        this.type = in.readUTF();
        this.transferPlace = in.readUTF();
        this.kind = in.readUTF();
        this.position = in.readUTF();
        this.deviceID = in.readUTF();
        this.time = in.readUTF();
        this.amount = in.readDouble();
        this.balance = in.readDouble();

        this.work = in.readUTF();
    }

    public String toString(){
        return new StringBuilder().append(studentID).append(",").append(nation).append(",")
                .append(gender).append(",").append(political).append(",").append(college).append(",")
                .append(major).append(",").append(type).append(",").append(transferPlace).append(",")
                .append(kind).append(",").append(position).append(",").append(deviceID).append(",")
                .append(time).append(",").append(amount).append(",").append(balance).append(",").append(work)
        .toString();
    }

    public GraduateStudentsConsumeAndBasicInfoRecord(String toString){
        String[] array = toString.split(",", -1);
        studentID = array[0];
        nation = array[1];
        gender = array[2];
        political = array[3];
        college = array[4];
        major = array[5];
        type = array[6];
        transferPlace = array[7];
        kind = array[8];
        position = array[9];
        deviceID = array[10];
        time = array[11];
        amount = Double.parseDouble(array[12]);
        balance = Double.parseDouble(array[13]);
        work = array[14];
    }
}
