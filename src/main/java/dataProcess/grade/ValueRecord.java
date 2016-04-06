package dataProcess.grade;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 16-3-21.
 * 每个主键所对应的值的记录.
 */
public class ValueRecord implements Writable{

    private String syear;
    private String term;
    private String classNature;
    private String className;
    private double grade;
    private double credit;
    private short renovate; //重修标志位,1代表重修.
    private double resitGrade;//补考成绩，如果有的話.

    /**
     * Serialize must implements the default constractor
     */
    public ValueRecord(){}

    public ValueRecord(String syear,String term,String classNature, String className,double grade,double credit,short renovate,double resitGrade){
        this.syear = syear;
        this.term = term;
        this.classNature = classNature;
        this.grade = grade;
        this.credit = credit;
        this.renovate = renovate;
        this.resitGrade = resitGrade;
        this.className = className;
    }

    public String getSyear(){
        return syear;
    }
    public String getTerm(){
        return term;
    }
    public String getClassNature(){
        return classNature;
    }
    public String getClassName(){
        return className;
    }
    public double getGrade(){
        return grade;
    }
    public double getCredit(){
        return credit;
    }
    public short getRenovate(){
        return renovate;
    }
    public double getResitGrade(){
        return resitGrade;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(syear);
        out.writeUTF(term);
        out.writeUTF(classNature);
        out.writeUTF(className);
        out.writeDouble(grade);
        out.writeDouble(credit);
        out.writeShort(renovate);
        out.writeDouble(resitGrade);
    }

    public void readFields(DataInput in) throws IOException {
        syear = in.readUTF();
        term = in.readUTF();
        classNature = in.readUTF();
        className = in.readUTF();
        grade = in.readDouble();
        credit = in.readDouble();
        renovate = in.readShort();
        resitGrade = in.readDouble();
    }
}
