package dataProcess.consume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by sghipr on 4/8/16.
 */
public class DeduplicationJob {

    private static String DuOutPut = "/home/sghipr/careerPredict/duOutPut";

    public static Path runDeduplicationJob(Path input, Configuration baseConf) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(baseConf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(DeduplicationJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Deduplication.Key.class);
        job.setMapOutputValueClass(Deduplication.Record.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Deduplication.DeMapper.class);
        job.setReducerClass(Deduplication.DeReduce.class);
        job.setPartitionerClass(Deduplication.KeyPartition.class);
        job.setGroupingComparatorClass(Deduplication.KeyGroupCompartor.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job,input);
        Path output = new Path(DuOutPut);
        FileSystem.get(conf).delete(output,true);
        FileOutputFormat.setOutputPath(job,output);

        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("RunDeduplicationJob Failed!");
            System.exit(1);
        }
        return output;
    }
}
