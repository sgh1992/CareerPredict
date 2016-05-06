package analysize;

import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by sghipr on 5/6/16.
 *  根据MR来运行UniqueKindJob
 *  主要是为了配合后续的JobControl来协调作业的运行.
 *  单独地使用多线程地处理方式也是可行的.
 */
public class UniqueKindMR {

    public static class UniqueKindMapper extends Mapper<LongWritable,Text, Text, NullWritable>{

        private Set<String> placeSet;
        public void setup(Context context){
            placeSet = new HashSet<>();
        }

        public void map(LongWritable key, Text text, Context context){
            GraduateStudentsConsumeAndBasicInfoRecord record = new GraduateStudentsConsumeAndBasicInfoRecord(text.toString());
            if(!placeSet.contains(record.getKind()))
                placeSet.add(record.getKind());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(String placeKind : placeSet)
                context.write(new Text(placeKind), NullWritable.get());
        }
    }

    public static class UniqueKindReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
