package analysize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sghipr on 5/5/16.
 */
public class AmountAndCountVectorRunnable implements Callable<Path>{

    private Configuration conf;
    private Path input;
    private String startTime;
    private String endTime;
    private String rankYear;

    public AmountAndCountVectorRunnable(Configuration conf, Path input, String startTime, String endTime, String rankYear){
        this.conf = conf;
        this.input = input;
        this.startTime = startTime;
        this.endTime = endTime;
        this.rankYear = rankYear;
    }

    @Override
    public Path call() throws Exception {
        return AnalysizeJob.runConsumePlaceAnalysizeJob(conf,input,startTime,endTime,rankYear);
    }
}
