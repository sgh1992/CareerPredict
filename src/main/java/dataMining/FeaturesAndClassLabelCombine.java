package dataMining;

import dataProcess.tool.GraduateStudentBasicRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by sghipr on 5/12/16.
 * 将已经获得的特征与学生的去向类别信息相统一.
 */
public class FeaturesAndClassLabelCombine {

    static enum GraduateStudentMissing{
        MISSING;
    }

    public static class FeatureAndClassLabelsCombineMapper extends Mapper<Text,VectorWritable,Text, VectorWritable>{

        private HashMap<String,String> careersMap;
        public void setup(Context context) throws IOException {
            careersMap = getCareersMap(context.getConfiguration(), new Path(context.getCacheFiles()[0]));
        }
        public HashMap<String,String> getCareersMap(Configuration conf, Path graduateStudentsInfo) throws IOException {
            HashMap<String, String> careerMap = new HashMap<>();
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),graduateStudentsInfo,conf);
            NullWritable key = NullWritable.get();
            GraduateStudentBasicRecord gbr = (GraduateStudentBasicRecord) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            while(reader.next(key,gbr)){
                careerMap.put(gbr.getStudentID(),gbr.getWork());
            }
            reader.close();
            return careerMap;
        }
        public void map(Text studentID, VectorWritable vector, Context context) throws IOException, InterruptedException {
            if(careersMap.containsKey(studentID.toString())){

                context.write(new Text(careersMap.get(studentID.toString())), vector);
            }
            else{
                context.getCounter(GraduateStudentMissing.MISSING).increment(1);
            }
        }
    }
}
