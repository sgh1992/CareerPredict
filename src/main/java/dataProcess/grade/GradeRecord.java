package dataProcess.grade;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sghipr on 16-3-21.
 * 每条成绩记录.
 */
public class GradeRecord {

    private String sid;
    private String syear;
    private String term;
    private String className;
    private String classNature;
    private double grade;
    private double credit;
    private short renovate; //重修标志位,1代表重修.
    private double resitGrade;//补考成绩，如果有的話.
    private List<String> renovateList;
    private List<String> termList;

    public String getSid(){
        return sid;
    }
    public String getSyear(){
        return syear;
    }
    public String getTerm(){
        return term;
    }
    public String getClassName(){
        return className;
    }
    public String getClassNature(){
        return classNature;
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


    public GradeRecord(){
        renovateList = new ArrayList<String>();
        renovateList.add("0");
        renovateList.add("1");

        termList = new ArrayList<String>();
        termList.add("1");
        termList.add("2");
    }

    /**
     * 解析一条字符串.
     * @param line
     * @return
     */
    public boolean parser(String line){
        String[] array = line.split(",", -1);
        if(array.length < 14)
            return false;
        syear = array[0].trim();
        String termStr = array[1].trim();
        if(!termList.contains(termStr))
            return false;
        term = termStr;
        sid = array[2].trim();
        className = array[5].trim();
        credit = Double.parseDouble(array[6].trim());
        grade = Double.parseDouble(array[7].trim());
        if(grade < 0 || grade > 100)
            return false;
        String renovateStr = array[8].trim();
        if(!renovateList.contains(renovateStr))
            return false;
        renovate = Short.parseShort(renovateStr);
        resitGrade = Double.parseDouble(array[12].trim());
        classNature = array[13].trim();
        return  true;
    }

    public GradeRecord(String sid, String sYear,String term,String classNature, String className,double grade, double credit,short renovate, double resitGrade){
        this.sid = sid;
        this.syear = sYear;
        this.term = term;
        this.classNature = classNature;
        this.className = className;
        this.grade = grade;
        this.credit = credit;
        this.renovate = renovate;
        this.resitGrade = resitGrade;
    }
}
