package dataMining;

import dataProcess.FeatureNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.classifier.sgd.L2;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PriorFunction;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by sghipr on 16-3-24.
 *
 */
public class LogisticRegression {

    private Configuration conf;
    private OnlineLogisticRegression classifier;
    private int numCategories;
    private int numFeatures;
    private static String PREDICT = "predict";
    /**
     * 正則化方式的选择,可以是L1范数,也可以是L2范数
     */
    private PriorFunction prior;

    /**
     * 标签的映射,将字符串映射为其0 - numCategories-1的值.
     */
    private HashMap<String,Integer> labels;

    /**
     * 上述 map表的反向索引
     */
    private HashMap<Integer,String> reverse;

    public LogisticRegression(Configuration conf){
        this.conf = conf;
    }

    public void buildClassify(Path trains){
        //包含了初始化的过程.
        HashMap<String,String> studentCareers = getCareer();

        classifier = new OnlineLogisticRegression(numCategories,numFeatures,new L2());
        int MISSCAREER = 0;
        try {
            List<Path> filePathes = new ArrayList<Path>();
            getPathes(trains, FileSystem.get(conf), filePathes);
            for(Path path : filePathes){
                SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),path,conf);
                Text sid = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
                VectorWritable featureVector = (VectorWritable)ReflectionUtils.newInstance(reader.getValueClass(),conf);
                while(reader.next(sid,featureVector)){
                    String career = studentCareers.get(sid.toString());
                    if(career != null){
                        //System.err.printf("career:%s,labels.get:%d\n",career,labels.get(career));
                        classifier.train(labels.get(career), featureVector.get());
                    }
                    else{
                        MISSCAREER++;
                    }
                }
                reader.close();
            }
        } catch (IOException e) {
            System.err.println("Error in FileSystem.getConf!");
            e.printStackTrace();
            System.exit(1);
        }

        System.err.println("the student num of could not found Career is: " + MISSCAREER);
    }

    /**
     * DFS来获得其每个目录下所有的文件信息.
     * @param trains
     * @param fs
     * @param filePathes
     */
    public void getPathes(Path trains,FileSystem fs,List<Path> filePathes){
        try {
            for(FileStatus status : fs.listStatus(trains)){
               if(status.isFile())
                   filePathes.add(status.getPath());
                else
                   getPathes(status.getPath(),fs,filePathes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path classifierForAllInstances(Path test){
        //当前测试集只是在上次向量的输出目录中,还需要进一步得到它的上层目录.
        Path predictResult = new Path(test.getParent().getParent(),PREDICT);
        classifierForAllInstances(test,predictResult);
        return predictResult;
    }

    /**
     * 对整个测试文件进行分类.
     * @param test
     * @param predictResult
     */
    public void classifierForAllInstances(Path test,Path predictResult){
        List<Path> testFiles = new ArrayList<Path>();
        try {
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer writer = new SequenceFile.Writer(fs,conf,predictResult,Text.class,Text.class);
//            BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(fs.create(predictResult)));
            getPathes(test, FileSystem.get(conf), testFiles);
            for(Path path : testFiles){
                SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
                Text sid = (Text)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
                VectorWritable featureVector = (VectorWritable)ReflectionUtils.newInstance(reader.getValueClass(),conf);
                while(reader.next(sid,featureVector)){
                    String predict = classifierForInstance(featureVector.get());
                    writer.append(sid,new Text(predict));
                }
                reader.close();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 返回预测的结果.
     * @param instance
     * @return
     */
    public String classifierForInstance(Vector instance){
        Vector result = classifier.classify(instance);
        double maxPro = result.get(result.maxValueIndex());
        double label0Pro = 1 - result.zSum(); //注意,返回的预测结果中只有K － 1个结果.

        if(maxPro >= label0Pro) {
            //System.err.println(reverse.get(result.maxValueIndex() + 1));
            return reverse.get(result.maxValueIndex() + 1);//注意,此处的返回结果需要加上1.
        }
        else {
            //System.err.println(reverse.get(0));
            return reverse.get(0);
        }
    }

    /**
     * 这种设计方式可能有些问题!!!
     * 与另外的对象FeatureNames嵌套的太深了.
     * @return
     */
    public HashMap<String,String> getCareer(){
        FeatureNames featureNames = new FeatureNames(conf);
        return getCareer(featureNames.getCareerInfo());
    }

    /**
     * 获得学生的就业信息!!!
     * @param career
     * @return
     */
    public HashMap<String,String> getCareer(Path career){
        HashMap<String,String> studentCareer = new HashMap<String, String>();
        FeatureNames featureNames = new FeatureNames(conf);
        numFeatures = featureNames.featureNums();
        labels = new HashMap<String, Integer>();
        reverse = new HashMap<Integer, String>();
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf),career,conf);
            Text sid = (Text)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Text graduate = (Text)ReflectionUtils.newInstance(reader.getValueClass(),conf);
            while(reader.next(sid,graduate)){
                studentCareer.put(sid.toString(),graduate.toString());
                if (!labels.containsKey(graduate.toString())){
                    reverse.put(labels.size(),graduate.toString());
                    labels.put(graduate.toString(),labels.size());
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        numCategories = labels.size();
        return studentCareer;
    }
}
