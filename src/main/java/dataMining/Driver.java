package dataMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sghipr on 5/12/16.
 */
public class Driver extends Configured implements Tool{

    private List<Integer> normalizedIndexs;

    public Driver(List<Integer> normalizedIndexs){
        this.normalizedIndexs = normalizedIndexs;
    }

    @Override
    public int run(String[] args) throws Exception {
        dataProcess.tool.Tool tool = new dataProcess.tool.Tool(getConf());
        Path features = new Path("/home/sghipr/careerPredict/combineBasicInfoOutPut");
        Path trainData = runTrainDataJob(getConf(), features, tool.getGraduateStudentsBasicPath(dataProcess.consume.Driver.GraduateStudentsBasicInfo));
        Path normalizedData = runNormalizeJob(getConf(),trainData,normalizedIndexs);
        return 0;
    }
    public Path runTrainDataJob(Configuration conf, Path features, Path graduateStudentsInfo) throws InterruptedException, IOException, ClassNotFoundException {
        return DataMiningJob.runTrainDataJob(conf,features,graduateStudentsInfo);
    }

    public Path runNormalizeJob(Configuration conf, Path trains, List<Integer> normalizedIndexs) throws IOException {
        return DataMiningJob.runAllNormalizeJob(conf,trains,normalizedIndexs);
    }
    public static void main(String[] args) throws Exception {

        List<Integer> normalizedIndex = new ArrayList<>();
        for(int i = 0; i <= 13; i++)
            normalizedIndex.add(i);
        ToolRunner.run(new Driver(normalizedIndex),args);
    }
}
