package dataProcess.grade;

import dataMining.LogisticRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.sgd.L1;

import java.io.IOException;

/**
 * Created by sghipr on 16-3-22.
 */
public class Driver extends Configured implements Tool {

    private static String GRADEINPUT = "/usr/local/careerPredict/grade.csv";

    public void setGRADEINPUT(String gradeinput){
        GRADEINPUT = gradeinput;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /**
         * 成绩向量.
         */
        Path gradeVector = runGrade(conf,new Path(GRADEINPUT));
        //Path predictResult = runClassifier(new Path(gradeVector,"train"),new Path(gradeVector,"test"),conf);
        return 0;
    }

    /**
     * 根据训练集训练出分类器,根据测试集得出预测结果.
     * @param trains
     * @param test
     * @param conf
     * @return
     */
    public Path runClassifier(Path trains, Path test, Configuration conf){
        LogisticRegression logisticRegression = new LogisticRegression(conf);
        logisticRegression.buildClassify(trains);
        return logisticRegression.classifierForAllInstances(test);
    }

    public Path runGrade(Configuration conf,Path input){
        GradeVector gradeVector = new GradeVector();
        Path output = null;
        try {
            output = gradeVector.runGradeJob(conf,input);
        } catch (IOException e) {
            System.err.println(e);
            System.exit(1);
        } catch (ClassNotFoundException e) {
           System.err.println(e);
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println(e);
            System.exit(1);
        }
        return output;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(),args);
    }
}
