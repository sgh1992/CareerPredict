package dataProcess.grade;

import dataProcess.FeatureNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by sghipr on 16-3-22.
 */
public class GradeVector {
    /**
     * 自定义的分组方式.
     */
    static class GroupingByFirstKey extends WritableComparator {
        public GroupingByFirstKey(){
            super(IDClassNameRenovate.class,true);
        }

        public int compare(WritableComparable o1, WritableComparable o2){
            IDClassNameRenovate idClassNameRenovate1 = (IDClassNameRenovate)o1;
            IDClassNameRenovate idClassNameRenovate2 = (IDClassNameRenovate)o2;
            return idClassNameRenovate1.getSid().compareTo(idClassNameRenovate2.getSid());
        }
    }

    /**
     * 自定义分区方式
     */
    static class PartitionByFirstKey extends Partitioner<IDClassNameRenovate,ValueRecord>{

        @Override
        public int getPartition(IDClassNameRenovate key, ValueRecord value, int numPartitions) {
            return Math.abs(key.getSid().hashCode()) % numPartitions;//注意其hashCode可能会超出.因此需要使用绝对值.
        }
    }

    /**
     * 自定义排序的方式.
     */
    static class SortByAllKey extends WritableComparator{
        public SortByAllKey(){
            super(IDClassNameRenovate.class,true);
        }

        public int compare(WritableComparable o1,WritableComparable o2){
            IDClassNameRenovate key1 = (IDClassNameRenovate)o1;
            IDClassNameRenovate key2 = (IDClassNameRenovate)o2;
            return key1.compareTo(key2);
        }
    }

    /**
     * 默认的成绩特征向量的输出目录
     */
    private static String GRADEVECTOR = "gradevector";

    /**
     * 运行成绩的map-Reduce作业.
     * 返回输出结果.
     * @param baseConf
     * @param input
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public Path runGradeJob(Configuration baseConf,Path input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(baseConf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(GradeVector.class);
        /**
         * 设置缓存.
         */
        FeatureNames featureNames = new FeatureNames(conf);
        job.addCacheFile(featureNames.title().toUri());
        job.addCacheFile(featureNames.getEntryYear().toUri());

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(IDClassNameRenovate.class);
        job.setMapOutputValueClass(ValueRecord.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setMapperClass(GradeMapper.class);
        job.setReducerClass(GradeReducer.class);
        job.setPartitionerClass(PartitionByFirstKey.class);
        job.setSortComparatorClass(SortByAllKey.class);
        job.setGroupingComparatorClass(GroupingByFirstKey.class);

        job.setNumReduceTasks(3);//设置Reduce的个数有利于負載均衡!!!

        FileInputFormat.addInputPath(job,input);
        Path output = new Path(input.getParent(),GRADEVECTOR);
        FileSystem.get(conf).delete(output,true);
        FileOutputFormat.setOutputPath(job,output);
        boolean success = job.waitForCompletion(true);
        if(!success){
            System.err.println("runGradeJob fails!!!");
            System.exit(1);
        }
        return output;
    }
}
