package dataProcess.consume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by sghipr on 4/8/16.
 */
public class Driver extends Configured implements Tool{

    private static String ORIGIALCONSUMEDATA = "hdfs://Master:9000/user/sghipr/careerPredict/consumeData.csv";

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        setConf(conf);
        Path duOutPut = DeduplicationJob.runDeduplicationJob(new Path(ORIGIALCONSUMEDATA),getConf());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(),args);
    }
}
