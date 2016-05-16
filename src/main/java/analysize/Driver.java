package analysize;

import dataProcess.consume.ConsumeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by sghipr on 5/4/16.
 */
public class Driver extends Configured implements Tool{

    private String startTime;
    private String endTime;
    private String rankYear;

    public Driver(String startTime, String endTime, String rankYear){
        this.startTime = startTime;
        this.endTime = endTime;
        this.rankYear = rankYear;
    }

    @Override
    public int run(String[] args) throws Exception {
        //首先模拟运行.
        Path graduateStudentsForConsumeAndBasicInfo = new Path("/user/sghipr/careerPredict/graduateStudentsForConsumeAndBasicInfoOutPut");
        //Path consumePlaceVector = runConsumePlaceVectorJob(getConf(), graduateStudentsForConsumeAndBasicInfo);
        //Path consumePlaceVector = new Path("/user/sghipr/careerPredict/consumePlaceVector");
        //runCombineBasicInfo(getConf(), consumePlaceVector);
        runGraduateStudentConusmeAndBasicInfo_PlaceVector_CombineBasic_ControlJob(getConf());
        return 0;
    }

    /**
     * 获得这个Path的工作是由多个mapReduce作业组成的.
     * 这里就产生出一种实际的生产模式,即对某些复杂的工作来说有可能是由多个mapReduce作业来进行处理的.
     * 而这种情况应该是实际生产环境中经常用到的，因此就需要使用JobControl来协调这些mapReduce
     * JobControl本质上就是运用的多线程技术来处理的.
     * 但是在这里，我采用了另外一种显示调用java多线程的技术来处理这种情况.
     * @param conf
     * @param graduateStudentsForConsumeAndBasicInfo
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Path runConsumePlaceVectorJob(Configuration conf, Path graduateStudentsForConsumeAndBasicInfo) throws ExecutionException, InterruptedException, IOException, ClassNotFoundException {
        ExecutorService exec = Executors.newCachedThreadPool();
        Future<Path> amountAndCountVector = exec.submit(new AmountAndCountVectorRunnable(conf,graduateStudentsForConsumeAndBasicInfo,startTime,endTime,rankYear));
        Future<Path> consumePlace = exec.submit(new UniqueKind(graduateStudentsForConsumeAndBasicInfo,conf));
        //这里是一种显示的阻塞.
        return AnalysizeJob.runconsumePlaceVectorJob(conf,amountAndCountVector.get(),consumePlace.get());
    }
    public Path runCombineBasicInfo(Configuration conf, Path features) throws IOException, InterruptedException, ClassNotFoundException {
        dataProcess.tool.Tool tool = new dataProcess.tool.Tool(conf);
        return AnalysizeJob.runCombineBasicInfoJob(conf, features, tool.getGraduateStudentsBasicPath(dataProcess.consume.Driver.GraduateStudentsBasicInfo),tool.graduateStudentsBasicMap(dataProcess.consume.Driver.GraduateStudentsBasicInfo));
    }

    public Path runGraduateStudentConusmeAndBasicInfo_PlaceVector_CombineBasic_ControlJob(Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {

        dataProcess.tool.Tool tool = new dataProcess.tool.Tool(conf);
        Path duOut = new Path("/user/sghipr/careerPredict/duOutPut");
        Path graduateStudentsBasicInfo = tool.getGraduateStudentsBasicPath(dataProcess.consume.Driver.GraduateStudentsBasicInfo);
        Path graduateStudentsBasicMap = tool.graduateStudentsBasicMap(dataProcess.consume.Driver.GraduateStudentsBasicInfo);
        //Job
        Job graduateStudentConsumeAndBasicInfoJob = ConsumeJob.graduateStudentsForConsumeAndBasicInfoJob(duOut,graduateStudentsBasicInfo,tool.consumePlaceTransfer(dataProcess.consume.Driver.ConsumePlace),conf);
        Job consumeAmountAndCountAnalysizeJob = AnalysizeJob.consumePlaceAnalysizeJob(conf, FileOutputFormat.getOutputPath(graduateStudentConsumeAndBasicInfoJob), startTime,endTime, rankYear);
        Job uniqueKindJob = AnalysizeJob.uniqueKindJob(conf,FileOutputFormat.getOutputPath(graduateStudentConsumeAndBasicInfoJob));
        Job consumeVectorJob = AnalysizeJob.consumePlaceVectorJob(conf, FileOutputFormat.getOutputPath(consumeAmountAndCountAnalysizeJob), FileOutputFormat.getOutputPath(uniqueKindJob));
        Job basicInfoCombineJob = AnalysizeJob.combineBasicInfoJob(conf,FileOutputFormat.getOutputPath(consumeVectorJob),graduateStudentsBasicInfo, graduateStudentsBasicMap);

        //Control Job
        ControlledJob cGraduateStudentConsumeAndBasicInfoJob = new ControlledJob(graduateStudentConsumeAndBasicInfoJob,null);
        ControlledJob cConsumeAmountAndCountAnalysizeJob = new ControlledJob(consumeAmountAndCountAnalysizeJob, null);
        ControlledJob cUniqueKindJob = new ControlledJob(uniqueKindJob,null);
        ControlledJob cConsumeVectorJob = new ControlledJob(consumeVectorJob,null);
       ControlledJob cBasicInfoCombineJob = new ControlledJob(basicInfoCombineJob,null);

        //dependnecy
        cConsumeAmountAndCountAnalysizeJob.addDependingJob(cGraduateStudentConsumeAndBasicInfoJob);
        cUniqueKindJob.addDependingJob(cGraduateStudentConsumeAndBasicInfoJob);

        cConsumeVectorJob.addDependingJob(cConsumeAmountAndCountAnalysizeJob);
        cConsumeVectorJob.addDependingJob(cUniqueKindJob);

        cBasicInfoCombineJob.addDependingJob(cConsumeVectorJob);

        //Running
        JobControl jobControl = new JobControl("consumeBasicInfo_PlaceVector_CombineBasic");
        jobControl.addJob(cGraduateStudentConsumeAndBasicInfoJob);
        jobControl.addJob(cConsumeAmountAndCountAnalysizeJob);
        jobControl.addJob(cUniqueKindJob);
        jobControl.addJob(cConsumeVectorJob);
        jobControl.addJob(cBasicInfoCombineJob);

        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(jobControl);

        while (true){
            if(jobControl.allFinished()){
                System.err.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                return FileOutputFormat.getOutputPath(basicInfoCombineJob);
            }
            if(jobControl.getFailedJobList().size() > 0){
                jobControl.stop();
                System.err.println(jobControl.getFailedJobList());
                System.exit(1);
            }
        }
    }

    /**
     * 注意，这是一个测试接口.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String startTime = "20100901";
        String endTime = "20110201";
        String rankYear = "2010";
        ToolRunner.run(new Driver(startTime, endTime, rankYear),args);
    }
}