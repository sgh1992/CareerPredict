package dataProcess;

import dataBase.DB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sghipr on 16-3-21.
 *映射,将字符串映射类整形实数.
*/
public class FeatureNames{
    //private static Path GRADE = null;
    private static String FEATURE = "/usr/local/careerPredict/featureTitle";
    private static String STUDENTBASICINFO = "/usr/local/careerPredict/studentbasicinfo";
    private static String CAREER = "/usr/local/careerPredict/careerInfo";
    private Configuration conf;
    public FeatureNames(Configuration conf){
        this.conf = conf;
    }

    public Path title(){
        HashMap<String,Integer> titleMap = gradeFeatures();
        /**
         * 有点需要注意,在特征向量中并不需要学号这个字段.
         */
        //titleMap.put("sid",titleMap.size());
        return writeTitleMap(titleMap);
    }

    /**
     * 总共的特征的个数.不包括学号.
     * @return
     */
    public int featureNums(){
        return gradeFeatures().size();
    }

    public Path writeTitleMap(HashMap<String,Integer> titleMap){
        try {
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(FEATURE), Text.class, IntWritable.class);
            for(Map.Entry<String, Integer> entry : titleMap.entrySet())
                writer.append(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            writer.close();
        } catch (IOException e) {
            System.err.println(e);
            System.exit(1);
        }
        return new Path(FEATURE);
    }
    public List<Integer> terms(){
        List<Integer> termList = new ArrayList<Integer>();
        for(int i = 1; i <= 8; i++)
            termList.add(i);
        return termList;
    }

    public HashMap<String,Integer> gradeFeatures(){

        HashMap<String, Integer> titleMap = new HashMap<String, Integer>();
        List<Integer> terms = terms();
        List<String> classNatures = new ArrayList<String>();
        try {
            DB db = new DB();
            Connection conn = db.getConnection();
            String sql = "select distinct classNature from studentallgrade";
            Statement stat = conn.createStatement();
            ResultSet set = stat.executeQuery(sql);
            while(set.next()){
                classNatures.add(set.getString("classNature"));
            }
            set.close();
            stat.close();
            conn.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        for(int term : terms){
            for(String classNature : classNatures){
                //以这种方式进行硬编码,可能会些问题.那么在mapReduce过程中，也必须保证与这里的字段完全一致!
                String grade = term + "_" + classNature + "_" + "grade";
                String makeUp = term + "_" + classNature + "_" + "makeUpCount";//挂科次数
                String renovate = term + "_" + classNature + "_" + "renovateCount";//重修次数.
                String refresh = term + "_" + classNature + "_" + "refreshCount";//刷分次数.
                titleMap.put(grade,titleMap.size());
                titleMap.put(makeUp,titleMap.size());
                titleMap.put(renovate,titleMap.size());
                titleMap.put(refresh,titleMap.size());
            }
            String termGrade = term + "_" + "grade";
            String termMakeUpCount = term + "_" + "makeUpCount";
            String termRenovateCount = term + "_" + "renovateCount";
            String termRefreshCount = term + "_" + "refreshCount";
            titleMap.put(termGrade,titleMap.size());
            titleMap.put(termMakeUpCount,titleMap.size());
            titleMap.put(termRenovateCount,titleMap.size());
            titleMap.put(termRefreshCount,titleMap.size());
        }
        return titleMap;
    }

    public Path getEntryYear(){
        return getEntryYear(STUDENTBASICINFO);
    }

    /**
     * 返回每个学生所对应的入学年级.
     * @param basicInfoFile
     * @return
     */
    private Path getEntryYear(String basicInfoFile) {
        Path output = new Path(basicInfoFile);
        DB db = new DB();
        Connection conn = db.getConnection();
        String sql = "select sid,grade from new_studentbasicinfo";
        Statement stat = null;
        FileSystem fs = null;
        SequenceFile.Writer writer = null;
        try {
            stat = conn.createStatement();
            ResultSet set = stat.executeQuery(sql);
            fs = FileSystem.get(conf);
            writer = new SequenceFile.Writer(fs,conf,output,Text.class,Text.class);
            while(set.next()){
                writer.append(new Text(set.getString("sid")),new Text(set.getString("grade")));
            }
            set.close();
            stat.close();
            conn.close();
            writer.close();
        } catch (SQLException e) {
            System.err.println(e);
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e);
            System.exit(1);
        }
        return output;
    }

    public Path getCareerInfo(){
        return getCareerInfo(CAREER);
    }

    /**
     * 获得每个学生的毕业信息数据.
     * @param careerInfoOutFile
     * @return
     */
    public Path getCareerInfo(String careerInfoOutFile){
        Path output = new Path(careerInfoOutFile);
        DB db = new DB();
        Connection conn = db.getConnection();
        String sql = "select sid,work from graduateworkinfo";
        try {
            Statement stat = conn.createStatement();
            ResultSet set = stat.executeQuery(sql);
            SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf),conf,output,Text.class,Text.class);
            while(set.next()){
                writer.append(new Text(set.getString("sid")),new Text(set.getString("work")));
            }
            set.close();
            stat.close();
            conn.close();
            writer.close();
        } catch (SQLException e) {
            System.err.println("error in getCareerInfo sql!");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("IOException in getCareerInfo!");
            e.printStackTrace();
            System.exit(1);
        }
        return output;
    }

}
