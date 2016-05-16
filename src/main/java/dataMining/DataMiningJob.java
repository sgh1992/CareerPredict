package dataMining;

import dataProcess.tool.NormalizeData;
import dataProcess.tool.NormalizeRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by sghipr on 5/12/16.
 */
public class DataMiningJob {

    private static String featureAndCareerInfoCombine = "trains";

    public static Job trainDataJob(Configuration baseConf, Path input, Path graduateStudentsInfo) throws IOException {

        Job job = Job.getInstance(baseConf);
        job.addCacheFile(graduateStudentsInfo.toUri());
        job.setJarByClass(DataMiningJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setMapperClass(FeaturesAndClassLabelCombine.FeatureAndClassLabelsCombineMapper.class);

        job.setNumReduceTasks(0);

        Path output = new Path(input.getParent(), featureAndCareerInfoCombine);
        FileSystem.get(job.getConfiguration()).delete(output, true);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job,output);

        return job;
    }

    public static Path runTrainDataJob(Configuration baseConf, Path input, Path graduateStudentsInfo) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = trainDataJob(baseConf, input, graduateStudentsInfo);
        boolean success = job.waitForCompletion(true);

        if(!success){
            System.err.println("run TrainData Job failed!");
            System.exit(1);
        }

        return FileOutputFormat.getOutputPath(job);
    }

    public static Job meanAndStdJob(Configuration baseConf, Path trains, List<Integer> normalizedIndexs) throws IOException {
        Path serializeIndex =  NormalizeData.serializeNormalizeIndex(trains,normalizedIndexs,baseConf);
        Job  job = Job.getInstance(baseConf);
        job.addCacheFile(serializeIndex.toUri());

        job.setJarByClass(DataMiningJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NormalizeRecordWritable.class);

        job.setMapperClass(NormalizeData.MeanAndStdMapper.class);
        job.setCombinerClass(NormalizeData.MeanAndStdReducer.class);
        job.setReducerClass(NormalizeData.MeanAndStdReducer.class);

        Path output = new Path(trains.getParent(),NormalizeData.meanAndStd);
        FileSystem.get(job.getConfiguration()).delete(output,true);
        FileInputFormat.addInputPath(job, trains);
        FileOutputFormat.setOutputPath(job,output);
        return job;

    }

    public static Job normalizeDataJob(Configuration baseConf, Path trains, Path meanAndStd) throws IOException {
        Job job = Job.getInstance(baseConf);
        job.addCacheFile(meanAndStd.toUri());
        job.setJarByClass(DataMiningJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setMapperClass(NormalizeData.NormalizeMapper.class);
        job.setNumReduceTasks(0);

        Path output = new Path(trains.getParent(),NormalizeData.normalizedData);
        FileSystem.get(job.getConfiguration()).delete(output,true);
        FileInputFormat.addInputPath(job, trains);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static Path runAllNormalizeJob(Configuration baseConf, Path trains, List<Integer> normalizedIndexs) throws IOException {

        Job meanAndStdJob = meanAndStdJob(baseConf, trains, normalizedIndexs);
        Job normalizeJob = normalizeDataJob(baseConf, trains, FileOutputFormat.getOutputPath(meanAndStdJob));

        ControlledJob cmsJob = new ControlledJob(meanAndStdJob,null);
        ControlledJob cNormalizeJob = new ControlledJob(normalizeJob,null);

        cNormalizeJob.addDependingJob(cmsJob);

        JobControl jobControl = new JobControl("normalizeData");
        jobControl.addJob(cmsJob);
        jobControl.addJob(cNormalizeJob);

        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(jobControl);

        while (true){
            if(jobControl.allFinished()){
                System.out.println(jobControl.getSuccessfulJobList());
                return FileOutputFormat.getOutputPath(normalizeJob);
            }

            if(jobControl.getFailedJobList().size() > 0){
                System.err.println(jobControl.getFailedJobList());
                System.exit(1);
            }
        }
    }
}
