package dataProcess.grade;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sghipr on 16-3-21.
 */
public class GradeMapper extends  Mapper<LongWritable,Text, IDClassNameRenovate, ValueRecord>{

    enum InValid{
        MISSING
    }
    private GradeRecord gradeRecord;

    public void setup(Context context){
        gradeRecord = new GradeRecord();
    }

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        boolean flag = gradeRecord.parser(value.toString());
        if(flag){
            context.write(getKey(),getValue());
        }else{
            context.getCounter(InValid.MISSING).increment(1);//记录无效的数据.
        }
    }
    public IDClassNameRenovate getKey(){
        String sid = gradeRecord.getSid();
        String className = gradeRecord.getClassName();
        short renovate = gradeRecord.getRenovate();
        return new IDClassNameRenovate(sid,className,renovate);
    }
    public ValueRecord getValue(){
        String syear = gradeRecord.getSyear();
        String term = gradeRecord.getTerm();
        String classNature = gradeRecord.getClassNature();
        String className = gradeRecord.getClassName();
        double credit = gradeRecord.getCredit();
        double grade = gradeRecord.getGrade();
        short renovate = gradeRecord.getRenovate();
        double resitGrade = gradeRecord.getResitGrade();
        return new ValueRecord(syear,term,classNature,className,grade,credit,renovate,resitGrade);
    }
}
