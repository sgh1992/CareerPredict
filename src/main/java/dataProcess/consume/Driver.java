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

    /**
     * 使用默认的hdfs路径,因此不需要指明主机号与端口号.
     */
    private static String ORIGIALCONSUMEDATA = "/user/sghipr/careerPredict/consumeData.csv";
    private static String GraduateStudentsBasicInfo = "/user/sghipr/careerPredict/graduateworkinfo.csv";
    private static String ConsumePlace = "/user/sghipr/careerPredict/consumePlace.csv";

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        setConf(conf);
        //Path duOutPut = runDeduplicationJob(new Path(ORIGIALCONSUMEDATA),getConf());
        Path duOutPut = new Path("/user/sghipr/careerPredict/duOutPut");
        Path graduateStudentsForConsumeAndBasicInfoPath = runGraduateStudentsForConsumeAndBasicInfoJob(duOutPut,getConf());
        return 0;

    }

    public Path runDeduplicationJob(Path input,Configuration conf) throws Exception {
        return ConsumeJob.runDeduplicationJob(input,conf);
    }

    public Path runGraduateStudentsForConsumeAndBasicInfoJob(Path input,Configuration conf) throws Exception {

        dataProcess.tool.Tool tool = new dataProcess.tool.Tool(conf);
        return ConsumeJob.runGraduateStudentsForConsumeAndBasicInfoJob(input,tool.getGraduateStudentsBasicPath(GraduateStudentsBasicInfo),tool.consumePlaceTransfer(ConsumePlace),conf);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(),args);
    }
}
