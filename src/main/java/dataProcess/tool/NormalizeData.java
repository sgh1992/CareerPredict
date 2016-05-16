package dataProcess.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

/**
 * Created by sghipr on 5/13/16.
 * 归一化数据.
 * 利用z-score方法进行归一化.
 */
public class NormalizeData {

    public static String attributeIndexPath = "serializeNormalizeIndex";

    public static String meanAndStd = "DataMeanAndStd";

    public static String normalizedData = "normalizedData";

    public static Path serializeNormalizeIndex(Path trains, List<Integer> attributeIndexs, Configuration conf){

        Path output = new Path(trains.getParent(), attributeIndexPath);
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(conf),conf,output,IntWritable.class,NullWritable.class);
            for(int index : attributeIndexs)
                writer.append(new IntWritable(index),NullWritable.get());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return output;
    }

    /**
     * 第一阶段用于遍历整个数据集获得期望与方差.
     */
    public static class MeanAndStdMapper extends Mapper<Text,VectorWritable,IntWritable,NormalizeRecordWritable>{

        private Set<Integer> normalizeSet;

        public void setup(Context context) throws IOException {
            normalizeSet = getNormalizeSet(new Path(context.getCacheFiles()[0]),context.getConfiguration());
        }

        public Set<Integer> getNormalizeSet(Path path, Configuration conf) throws IOException {
            Set<Integer> normalizeSet = new HashSet<>();
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path,conf);
            IntWritable index = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            NullWritable value = NullWritable.get();
            while(reader.next(index,value)){
                normalizeSet.add(index.get());
            }
            reader.close();
            return normalizeSet;
        }

        public void map(Text key, VectorWritable features, Context context) throws IOException, InterruptedException {

            for(Vector.Element element : features.get().all()){
                if(normalizeSet.contains(element.index())){
                    NormalizeRecordWritable recordWritable = new NormalizeRecordWritable(element.index(),0,0);
                    recordWritable.addSum(element.get());
                    recordWritable.addSquare(element.get());
                    recordWritable.addNum();
                    context.write(new IntWritable(element.index()),recordWritable);
                }
            }
        }
    }

    public static class MeanAndStdReducer extends Reducer<IntWritable, NormalizeRecordWritable, IntWritable, NormalizeRecordWritable>{

        public void reduce(IntWritable index, Iterable<NormalizeRecordWritable> values, Context context) throws IOException, InterruptedException {
            NormalizeRecordWritable recordWritable = new NormalizeRecordWritable(index.get(),0,0);
            for(NormalizeRecordWritable record : values){
                recordWritable.updateSum(record.getSum());
                recordWritable.updateSquare(record.getSquareSum());
                recordWritable.addNum(record.getNum());
            }
            context.write(index, recordWritable);
        }
    }

    /**
     * 第二阶段根据获得的期望与方差来归一化数据.
     */

    public static class NormalizeMapper extends Mapper<Text, VectorWritable, Text,VectorWritable>{

        private HashMap<Integer,Double> mean;
        private HashMap<Integer,Double> std;

        public void setup(Context context) throws IOException {
            setMeanAndStd(new Path(context.getCacheFiles()[0]),context.getConfiguration());
        }

        public void setMeanAndStd(Path msPath, Configuration conf)  {
            mean = new HashMap<>();
            std = new HashMap<>();
            Tool tool = new Tool(conf);
            List<Path> paths = new ArrayList<>();
            try {
                tool.iteratePath(msPath, paths);
            } catch (IOException e) {
                e.printStackTrace();
            }
            SequenceFile.Reader reader = null;
            for(Path path: paths){
                try {
                    reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
                    IntWritable index = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
                    NormalizeRecordWritable normalizeRecordWritable = (NormalizeRecordWritable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
                    while (reader.next(index,normalizeRecordWritable)){
                        mean.put(index.get(),normalizeRecordWritable.getMean());
                        std.put(index.get(),normalizeRecordWritable.getStd());
                    }
                } catch (EOFException e) {
                    //e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(Text ID, VectorWritable features, Context context) throws IOException, InterruptedException {

            for(Vector.Element element : features.get().all()){
                if(std.containsKey(element.index()) && std.get(element.index()) != 0){
                    //z-Score
                    element.set((element.get() - mean.get(element.index()))/Math.sqrt(std.get(element.index())));
                }
            }
            context.write(ID,features);
        }
    }

}
