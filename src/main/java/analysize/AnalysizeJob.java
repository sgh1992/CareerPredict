package analysize;

import analysize.record.KeyPair;
import analysize.tool.CombineBasicInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by sghipr on 5/4/16.
 * 整个分析过程的工作流
 */
public class AnalysizeJob {

    /**
     * 存放每个消费地点每天的消费金额及消费天数的Vector
     **/
    public static String consumePlaceAnalysizeOutPut = "placeAmountAndCountOutPut";


    /**
     * 存放计算的每个学生在每个地点平均每天的消费情况.
     **/
    public static String consumePlaceVectorOutPut = "consumePlaceVector";

    public static String uniqueKindPlace = "uniqueKind";

    public static String combineBasicInfoOutPut = "combineBasicInfoOutPut";

    public static Job consumePlaceAnalysizeJob(Configuration baseConf, Path input, String startTime, String endTime, String rankYear) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(baseConf);
        job.getConfiguration().set("startTime",startTime);
        job.getConfiguration().set("endTime", endTime);
        job.getConfiguration().set("rankYear", rankYear);

        job.setJarByClass(AnalysizeJob.class);
        job.setMapperClass(ConsumePlaceAnalysize.consumePlaceAnalysizeMapper.class);
        job.setReducerClass(ConsumePlaceAnalysize.ConsumePlaceAnalysizeReduce.class);
        job.setCombinerClass(ConsumePlaceAnalysize.ConsumePlaceAnalysizeReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(KeyPair.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job,input);

        Path output = new Path(input.getParent(),consumePlaceAnalysizeOutPut);
        FileSystem.get(job.getConfiguration()).delete(output,true);

        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static Path runConsumePlaceAnalysizeJob(Configuration baseConf, Path input, String startTime, String endTime, String rankYear) throws InterruptedException, IOException, ClassNotFoundException {

        Job job = consumePlaceAnalysizeJob(baseConf,input, startTime, endTime, rankYear);
        boolean success = job.waitForCompletion(true);
        if(!success){
            System.err.println("runConsumePlaceAnalysizeJob failed!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }

    public static Job consumePlaceVectorJob(Configuration baseConf, Path input, Path consumePlace) throws IOException {

        Job job = Job.getInstance(baseConf);
        job.setJarByClass(AnalysizeJob.class);
        job.addCacheFile(consumePlace.toUri());

        job.setReducerClass(ConsumePlaceAnalysizeCombine.ConsumePlaceAnalysizeCombineReducer.class);
        job.setPartitionerClass(ConsumePlaceAnalysizeCombine.ConsumePlaceAnalysizeCombinePartition.class);
        job.setGroupingComparatorClass(ConsumePlaceAnalysizeCombine.ConsumePlaceAnalysizeCombineGroup.class);
        job.setSortComparatorClass(ConsumePlaceAnalysizeCombine.ConsumePlaceAnalysizeCombineSort.class);

        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(3);
        Path output = new Path(input.getParent(), consumePlaceVectorOutPut);
        FileSystem.get(job.getConfiguration()).delete(output, true);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        return job;
    }

    public static Path runconsumePlaceVectorJob(Configuration baseConf, Path input, Path consumePlace) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = consumePlaceVectorJob(baseConf, input, consumePlace);

        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("runConsumePlaceCombineJob failed!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }

    public static Job combineBasicInfoJob(Configuration baseConf, Path features, Path graduateStudentsBasicInfo, Path graduateStudentsBasicInfoMap) throws IOException {

        Job job = Job.getInstance(baseConf);

        job.setJarByClass(AnalysizeJob.class);

        job.addCacheFile(graduateStudentsBasicInfo.toUri());
        job.addCacheFile(graduateStudentsBasicInfoMap.toUri());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setMapperClass(CombineBasicInfo.CombineFeatureMapper.class);

        Path output = new Path(features.getParent(), combineBasicInfoOutPut);
        FileSystem.get(job.getConfiguration()).delete(output,true);

        FileInputFormat.addInputPath(job, features);
        FileOutputFormat.setOutputPath(job,output);

        job.setNumReduceTasks(3);

        return job;
    }

    public static Path runCombineBasicInfoJob(Configuration baseConf, Path features, Path graduateStudentsBasicInfo, Path graduateStudentsBasicInfoMap) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = combineBasicInfoJob(baseConf, features, graduateStudentsBasicInfo, graduateStudentsBasicInfoMap);
        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("runCombineBasicInfoJob failed!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }

    public static Job uniqueKindJob(Configuration baseConf, Path input) throws IOException {

        Job job = Job.getInstance(baseConf);
        job.setJarByClass(AnalysizeJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(UniqueKindMR.UniqueKindMapper.class);
        job.setReducerClass(UniqueKindMR.UniqueKindReducer.class);
        job.setCombinerClass(UniqueKindMR.UniqueKindReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path output = new Path(input.getParent(), uniqueKindPlace);
        FileSystem.get(job.getConfiguration()).delete(output,true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job,output);

        return job;
    }

    public static Path runUniqueKindJob(Configuration baseConf, Path input) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = uniqueKindJob(baseConf, input);

        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("run UniqueKindJob Failed!");
            System.exit(1);
        }

        return FileOutputFormat.getOutputPath(job);

    }



    }
