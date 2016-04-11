package dataProcess.consume;
import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import dataProcess.consume.record.Record;
import dataProcess.tool.GraduateStudentBasicRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by sghipr on 4/11/16.
 * 将已经毕业学生的消费信息提取出来,同时取出学生的基本信息数据.
 * 用作分析.
 **/
public class ConsumeForGraduatedStudents {

    public static String GraduateStudentsForConsumeAndBasicinfo = "graduateStudentsForConsumeAndBasicInfoOutPut";

    static class GraduatedStudentsMap extends Mapper<Text,Text,NullWritable,GraduateStudentsConsumeAndBasicInfoRecord>{
        private HashMap<String,GraduateStudentBasicRecord> graduatesInfo;
        public void setup(Context context) throws IOException {
            graduatesInfo = studentBasicInfo(new Path(context.getCacheFiles()[0]),context.getConfiguration());
        }

        public HashMap<String,GraduateStudentBasicRecord> studentBasicInfo(Path studentBasicInfo,Configuration conf){
            HashMap<String,GraduateStudentBasicRecord> basicRecords = new HashMap<String, GraduateStudentBasicRecord>();
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),studentBasicInfo,conf);
                NullWritable nullWritable = NullWritable.get();
                GraduateStudentBasicRecord recordInfo = (GraduateStudentBasicRecord)ReflectionUtils.newInstance(reader.getValueClass(),conf);
                while(reader.next(nullWritable,recordInfo)){
                    if(!basicRecords.containsKey(recordInfo.getStudentID()))
                        basicRecords.put(recordInfo.getStudentID(),recordInfo);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            return basicRecords;
        }

        public void map(Text studentKey,Text consume,Context context) throws IOException, InterruptedException {

            String studentID = studentKey.toString();
            Record consumeRecord = new Record(consume.toString());
            if(graduatesInfo.containsKey(studentID))
                context.write(NullWritable.get(), graduateStudentsConsumeCombineBasicInfo(studentID, consumeRecord));

        }
        public GraduateStudentsConsumeAndBasicInfoRecord graduateStudentsConsumeCombineBasicInfo(String studentID,Record consumeRecord){
            GraduateStudentBasicRecord basicRecord = graduatesInfo.get(studentID);
            return new GraduateStudentsConsumeAndBasicInfoRecord(consumeRecord,basicRecord);
        }
    }
}
