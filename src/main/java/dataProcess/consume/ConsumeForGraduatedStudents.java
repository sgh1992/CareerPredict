package dataProcess.consume;
import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import dataProcess.consume.record.Record;
import dataProcess.tool.GraduateStudentBasicRecord;
import dataProcess.tool.PlaceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.PlatformName;
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


    static enum MISSINGTransferPlace{
        MISSING_TRANSFER_PLACE;
    }
    static class GraduatedStudentsMap extends Mapper<Text,Text,NullWritable,GraduateStudentsConsumeAndBasicInfoRecord>{
        private HashMap<String,GraduateStudentBasicRecord> graduatesInfo;
        private HashMap<String,PlaceRecord> placeMap;

        public void setup(Context context) throws IOException {
            graduatesInfo = studentBasicInfo(new Path(context.getCacheFiles()[0]),context.getConfiguration());
            placeMap = getPlaceTransfer(new Path(context.getCacheFiles()[1]),context.getConfiguration());
        }

        public HashMap<String,GraduateStudentBasicRecord> studentBasicInfo(Path studentBasicInfo,Configuration conf){
            HashMap<String,GraduateStudentBasicRecord> basicRecords = new HashMap<String, GraduateStudentBasicRecord>();
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),studentBasicInfo,conf);
                NullWritable nullWritable = NullWritable.get();
                GraduateStudentBasicRecord recordInfo = (GraduateStudentBasicRecord)ReflectionUtils.newInstance(reader.getValueClass(),conf);
                while(reader.next(nullWritable,recordInfo)){
                    if(!basicRecords.containsKey(recordInfo.getStudentID()))
                    /**
                     * 注意，由于hadoop的序列化机制是循环使用同一个实例对象,因此在此处，需要进行深度复制.
                     */
                        basicRecords.put(recordInfo.getStudentID(),new GraduateStudentBasicRecord(recordInfo));
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            return basicRecords;
        }
        public HashMap<String,PlaceRecord> getPlaceTransfer(Path placePath,Configuration conf){
            HashMap<String,PlaceRecord> placeRecords = new HashMap<String, PlaceRecord>();
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),placePath,conf);
                NullWritable nullWritable = NullWritable.get();
                PlaceRecord record = (PlaceRecord)ReflectionUtils.newInstance(reader.getValueClass(),conf);
                while(reader.next(nullWritable,record)){
                    String origialPlace = record.getOrigialPlace();
                    placeRecords.put(origialPlace,new PlaceRecord(record));//注意，这里需要用深度拷贝.
                }
                reader.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return placeRecords;
        }

        public void map(Text studentKey,Text consume,Context context) throws IOException, InterruptedException {

            String studentID = studentKey.toString();
            Record consumeRecord = new Record(consume.toString());
            if(placeMap.containsKey(consumeRecord.getPlace())){
                if(graduatesInfo.containsKey(studentID)) {
                    context.write(NullWritable.get(), graduateStudentsConsumeCombineBasicInfo(studentID, consumeRecord,placeMap.get(consumeRecord.getPlace())));
                }
            }
            else{
                context.getCounter(MISSINGTransferPlace.MISSING_TRANSFER_PLACE).increment(1);
            }

        }
        public GraduateStudentsConsumeAndBasicInfoRecord graduateStudentsConsumeCombineBasicInfo(String studentID,Record consumeRecord,PlaceRecord placeRecord){
            GraduateStudentBasicRecord basicRecord = graduatesInfo.get(studentID);
            return new GraduateStudentsConsumeAndBasicInfoRecord(consumeRecord,basicRecord,placeRecord);
        }
    }
}
