package analysize;

import analysize.record.KeyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

    public static Job consumePlaceAnalysizeJob(Configuration baseConf, Path input, String startTime, String endTime, String rankYear) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(baseConf);
        conf.set("startTime",startTime);
        conf.set("endTime", endTime);
        conf.set("rankYear", rankYear);

        Job job = Job.getInstance(conf);
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
        FileSystem.get(conf).delete(output,true);

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

    public static Job consumePlaceCombineJob(Configuration baseConf, Path input, Path consumePlace) throws IOException {

        Configuration conf = new Configuration(baseConf);
        Job job = Job.getInstance(conf);

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
        FileSystem.get(conf).delete(output, true);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        return job;
    }

    public static Path runConsumePlaceCombineJob(Configuration baseConf, Path input, Path consumePlace) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = consumePlaceCombineJob(baseConf, input, consumePlace);

        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("runConsumePlaceCombineJob failed!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }
}
