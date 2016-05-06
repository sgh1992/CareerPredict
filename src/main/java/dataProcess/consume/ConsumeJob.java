package dataProcess.consume;

import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import dataProcess.consume.record.Key;
import dataProcess.consume.record.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by sghipr on 4/8/16.
 */
public class ConsumeJob {

    public static Job deduplicationJob(Path input, Configuration baseConf) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(baseConf);
        job.setJarByClass(ConsumeJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Record.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Deduplication.DeMapper.class);
        job.setReducerClass(Deduplication.DeReduce.class);
        job.setPartitionerClass(Deduplication.KeyPartition.class);
        job.setGroupingComparatorClass(Deduplication.KeyGroupCompartor.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, input);
        Path output = new Path(input.getParent(), Deduplication.DuOutPut);
        FileSystem.get(job.getConfiguration()).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static Path runDeduplicationJob(Path input, Configuration baseConf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = deduplicationJob(input,baseConf);

        boolean success = job.waitForCompletion(true);

        if (!success) {
            System.err.println("RunDeduplicationJob Failed!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }

        public static Job graduateStudentsForConsumeAndBasicInfoJob(Path input, Path studentBasicInfo, Path placeTransfer, Configuration baseConf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(baseConf);
        job.setJarByClass(ConsumeJob.class);
        job.setMapperClass(ConsumeForGraduatedStudents.GraduatedStudentsMap.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(GraduateStudentsConsumeAndBasicInfoRecord.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(studentBasicInfo.toUri());
        job.addCacheFile(placeTransfer.toUri());

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, input);
        Path output = new Path(input.getParent(), ConsumeForGraduatedStudents.GraduateStudentsForConsumeAndBasicinfo);
        FileSystem.get(job.getConfiguration()).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static Path runGraduateStudentsForConsumeAndBasicInfoJob(Path input, Path studentBasicInfo, Path placeTransfer, Configuration baseConf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = graduateStudentsForConsumeAndBasicInfoJob(input, studentBasicInfo, placeTransfer, baseConf);
        boolean success = job.waitForCompletion(true);
        while (!success) {
            System.err.println("GraduateStudentsForConsumeAndBasicInfoJob Failes!");
            System.exit(1);
        }
        return FileOutputFormat.getOutputPath(job);
    }
}


