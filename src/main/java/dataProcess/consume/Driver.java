package dataProcess.consume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

/**
 * Created by sghipr on 4/8/16.
 */
public class Driver extends Configured implements Tool {

    private static String ORIGIALCONSUMEDATA = "hdfs://Master:9000/user/sghipr/careerPredict/consumeData.csv";
    private static String GraduateStudentsBasicInfo = "hdfs://Master:9000/user/sghipr/careerPredict/graduateworkinfo.csv";

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        setConf(conf);
        Path duOutPut = runDeduplicationJob(new Path(ORIGIALCONSUMEDATA),getConf());
        Path graduateStudentsForConsumeAndBasicInfoPath = runGraduateStudentsForConsumeAndBasicInfoJob(duOutPut,getConf());
        return 0;
    }

    public Path runDeduplicationJob(Path input,Configuration conf) throws Exception {
        return ConsumeJob.runDeduplicationJob(input,conf);
    }

    public Path runGraduateStudentsForConsumeAndBasicInfoJob(Path input,Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {
        ;
        return ConsumeJob.runGraduateStudentsForConsumeAndBasicInfoJob(input,new dataProcess.tool.Tool(getConf()).getGraduateStudentsBasicPath(GraduateStudentsBasicInfo),conf);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(),args);
    }
}
