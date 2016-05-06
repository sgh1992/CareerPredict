package analysize.tool;

import dataProcess.tool.GraduateStudentBasicRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;

/**
 * Created by sghipr on 5/5/16.
 * 将之前所抽取的特征与每个人的基本信息相连接起来.
 * 主要的思路就是将用户的所有特征作为一个Vector来进行处理.
 **/
public class CombineBasicInfo {

    public static class CombineFeatureMapper extends Mapper<Text,VectorWritable,Text,VectorWritable>{
        private HashMap<String, GraduateStudentBasicRecord> graduateStudentBasicRecords;
        private HashMap<String, Integer> graduateStudentsBasicInfoMap;
        private int basicInfoSize;
        public void setup(Context context) throws IOException {

            Path basicRecordInfo = new Path(context.getCacheFiles()[0]);
            Path basicRecordMap = new Path(context.getCacheFiles()[1]);
            graduateStudentBasicRecords = getGraduateStudentBasicRecordInfo(context,basicRecordInfo);
            graduateStudentsBasicInfoMap = getStudentsBasicInfoMap(context,basicRecordMap);
            basicInfoSize = graduateStudentsBasicInfoMap.size();
        }

        public HashMap<String, GraduateStudentBasicRecord> getGraduateStudentBasicRecordInfo(Context context, Path graduateBasicInfoRecord) throws IOException {
            basicInfoSize = 0; //initalize
            HashMap<String,GraduateStudentBasicRecord> graduateStudentsBasicMap = new HashMap<>();
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(context.getConfiguration()),graduateBasicInfoRecord, context.getConfiguration());
            NullWritable key = NullWritable.get();
            GraduateStudentBasicRecord basicRecord = (GraduateStudentBasicRecord) ReflectionUtils.newInstance(reader.getValueClass(),context.getConfiguration());
            while(reader.next(key,basicRecord)){
                //注意，这里需要进行深度复制.
                graduateStudentsBasicMap.put(basicRecord.getStudentID(), new GraduateStudentBasicRecord(basicRecord));
            }
            reader.close();
            return graduateStudentsBasicMap;
        }

        public HashMap<String, Integer> getStudentsBasicInfoMap(Context context, Path path) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(path)));
            HashMap<String, Integer> basicMap = new HashMap<>();
            String str = null;
            while((str = reader.readLine()) != null) {
                String[] array = str.split("\t", -1);
                if(!basicMap.containsKey(array[0]))
                    basicMap.put(array[0], Integer.parseInt(array[1]));
            }
            reader.close();
            return basicMap;
        }

        public void map(Text ID, VectorWritable features, Context context) throws IOException, InterruptedException {
            int featureSize = features.get().size();//未合并学生基本特征前的特征个数.
            int size = featureSize + basicInfoSize;
            if(graduateStudentBasicRecords.containsKey(ID.toString())){
                Vector combineVector = new DenseVector(size);

                //features
                for(Vector.Element element : features.get().all())
                    combineVector.set(element.index(),element.get());

                //basicInfos
                List<String> personalInfos = graduateStudentBasicRecords.get(ID.toString()).iteratorAttribute();
                for(int i = 0; i < personalInfos.size(); i++){
                    if(graduateStudentsBasicInfoMap.containsKey(personalInfos.get(i))){
                        combineVector.set(i + graduateStudentsBasicInfoMap.get(personalInfos.get(i)), 1);
                    }
                    else{
                        combineVector.set(i + graduateStudentsBasicInfoMap.get("其它少数民族"), 1);
                    }
                }
                context.write(ID, new VectorWritable(combineVector));
            }
        }
    }
}
