package dataProcess.grade;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sghipr on 16-3-21.
 */
public class GradeReducer extends Reducer<IDClassNameRenovate, ValueRecord, Text, VectorWritable> {

    enum MissingRecord{
        NULL
    }

    class Evaluator{
        private int makeUpCount;//将之理解成不及格的课程数.
        private double grade;//总的成绩.
        private double credits;//总的学分.
        private int renovateCount;//重修的次数.
        private int refreshCount;//刷分的次数.
        public Evaluator(int makeUpCount, double grade,double credits, int renovateCount){
            this.makeUpCount = makeUpCount;
            this.grade = grade;
            this.renovateCount = renovateCount;
            this.credits = credits;
        }

        public Evaluator(Evaluator other){
            this(other.makeUpCount,other.grade,other.credits,other.renovateCount);
        }

        public Evaluator(double grade,double credits, short renovate) {
            this.grade = grade;
            if (grade < 60)
                makeUpCount++;
            if (renovate == 1)
                this.renovateCount = 1;
            else
                this.renovateCount = 0;
            this.credits = credits;
        }

        /**
         * 当相邻的两条记录不是同一个课程时，則更新
         * @param grade
         * @param credit
         * @param renovate
         */
        public void updateDiff(double grade, double credit,short renovate) {
            this.grade += grade;
            this.credits += credit;
            if (grade < 60)
                this.makeUpCount += 1;
            if (renovate == 1)
                this.renovateCount += 1;
        }
        public void update(Evaluator other){
            this.credits += other.credits;
            this.grade += other.grade;
            this.makeUpCount += other.makeUpCount;
            this.refreshCount += other.refreshCount;
            this.renovateCount += other.renovateCount;
        }
        /**
         * 下面的两个方法都是针对,当相邻的两条记录中的课程相同时,則只能单独地更新重修次数或刷分次数.
         */
        public void addRenovate(){
            this.renovateCount += 1;
        }
        public void addRefresh(){
            this.refreshCount += 1;
        }

        public double getAverage(){
            if(credits != 0)
                return grade/credits;
            return 0;
        }

        public int getMakeUpCount(){
            return makeUpCount;
        }
        public int getRenovateCount(){
            return renovateCount;
        }
        public int getRefreshCount(){
            return refreshCount;
        }

    }
    /**
     * 每个字符串标题所对应的映射下标.
     * 缓存在分布式缓存中.下同.
     */
    private HashMap<String,Integer> titleMap;

    /**
     * 每个学生所对应的入学年级.
     */
    private HashMap<String,String> entryYearMap;

    private MultipleOutputs<Text,VectorWritable> multipleOutputs;

    public void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        titleMap = initalTitle(context,new Path(uris[0]));
        entryYearMap = initalEntry(context,new Path(uris[1]));
        multipleOutputs = new MultipleOutputs<Text, VectorWritable>(context);
    }

    public HashMap<String,String> initalEntry(Context context, Path entryYearPath) throws IOException {
        HashMap<String,String> entryMap = new HashMap<String, String>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,entryYearPath,context.getConfiguration());
        Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(),context.getConfiguration());
        Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
        while(reader.next(key,value)){
            entryMap.put(key.toString(),value.toString());
        }
        reader.close();
        return entryMap;

    }

    public HashMap<String,Integer> initalTitle(Context context,Path titleMapPath) throws IOException {
        HashMap<String,Integer> titleMap = new HashMap<String, Integer>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,titleMapPath,context.getConfiguration());
        Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),context.getConfiguration());
        IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(),context.getConfiguration());
        while(reader.next(key,value)){
            titleMap.put(key.toString(),value.get());
        }
        reader.close();
        return titleMap;
    }

    public HashMap<Integer,HashMap<String,Evaluator>> evaluateGrades(String sid,Iterable<ValueRecord> iterable){
        if(!entryYearMap.containsKey(sid))
            return null;
        String entryYear = entryYearMap.get(sid);
        HashMap<Integer,HashMap<String,Evaluator>> termNatureEvaluators = new HashMap<Integer, HashMap<String, Evaluator>>();
        String beforeClassName = null;
        boolean refreshFlag = false;//刷分标志位.
        boolean sameClassFlag = false;//前后两门课程是否一致.
        boolean pass = true;
        boolean renovateFlag = false;
        for(ValueRecord valueRecord : iterable){
            String className = valueRecord.getClassName();
            double grade = valueRecord.getGrade();
            int termValue = term(entryYear,valueRecord.getSyear(),valueRecord.getTerm());
            if(termValue == -1)
                continue;
            if(className.equals(beforeClassName)) {
                sameClassFlag = true;
                if(pass)
                    refreshFlag = true;
                else
                    renovateFlag = true;
            }
            else {
                sameClassFlag = false;
                beforeClassName = className;
            }
            updateTermNatureEvaluators(termNatureEvaluators,termValue,valueRecord.getClassNature(),valueRecord.getGrade(),valueRecord.getCredit(),valueRecord.getRenovate(),refreshFlag,sameClassFlag,renovateFlag);
            if(grade >= 60)
                pass = true;
            else
                pass = false;
        }
        return termNatureEvaluators;
    }

    public Vector getVector(String sid,Iterable<ValueRecord> iterable){
        return getVector(evaluateGrades(sid,iterable));
    }

    public Vector getVector(HashMap<Integer,HashMap<String,Evaluator>> evaluators){
        if(evaluators == null)
            return null;
        Vector vector = new RandomAccessSparseVector(titleMap.size());
        for(Map.Entry<Integer,HashMap<String,Evaluator>> termEntry : evaluators.entrySet()){
            int term = termEntry.getKey();
            Evaluator termEvaluator = null;

            for(Map.Entry<String,Evaluator> classNatureEntry : termEntry.getValue().entrySet()){
                String classNature = classNatureEntry.getKey();
                Evaluator evaluator = classNatureEntry.getValue();
                //硬编码总是存在些问题.
                String gradeName = term + "_" + classNature + "_" + "grade";
                String makeUpCount = term + "_" + classNature + "_" + "makeUpCount";
                String renovateCount = term + "_" + classNature + "_" + "renovateCount";
                String refreshCount = term + "_" + classNature + "_" + "refreshCount";

                set(vector,evaluator.getAverage(),titleMap,gradeName);
                set(vector,evaluator.getMakeUpCount(),titleMap,makeUpCount);
                set(vector,evaluator.getRenovateCount(),titleMap,renovateCount);
                set(vector,evaluator.getRefreshCount(),titleMap,refreshCount);

                if(termEvaluator == null)
                    termEvaluator = new Evaluator(evaluator);
                else
                    termEvaluator.update(evaluator);
            }

            String termGrade = term + "_" + "grade";
            String termMakeUpCount = term + "_" + "makeUpCount";
            String termRenovateCount = term + "_" + "renovateCount";
            String termRefreshCount = term + "_" + "refreshCount";
            set(vector,termEvaluator.getAverage(),titleMap,termGrade);
            set(vector,termEvaluator.getMakeUpCount(),titleMap,termMakeUpCount);
            set(vector,termEvaluator.getRenovateCount(),titleMap,termRenovateCount);
            set(vector,termEvaluator.getRefreshCount(),titleMap,termRefreshCount);
        }
        return vector;
    }

    public void set(Vector vector,double value,HashMap<String,Integer> map,String name){
        if(!map.containsKey(name)){
            System.err.println("do not contains :" + name);
            System.exit(1);
        }
        vector.set(map.get(name),value);
    }

    public int term(String entryYear, String syear, String term){
        String[] firstAndSecond = syear.split("-", -1);
        int termValue = (Integer.parseInt(firstAndSecond[0]) - Integer.parseInt(entryYear)) * 2 + 1;
        if(termValue < 1 || termValue > 8)
            return -1;
        return termValue;
    }

    public void updateTermNatureEvaluators(HashMap<Integer,HashMap<String,Evaluator>> evaluators,int term,String classNature,double grade,double credit, short renovateValue,boolean refresh,boolean sameClassName,boolean renovate){
        if(!evaluators.containsKey(term))
            evaluators.put(term,new HashMap<String, Evaluator>());
        if(!evaluators.get(term).containsKey(classNature))
            evaluators.get(term).put(classNature,new Evaluator(grade,credit,renovateValue));
        if(sameClassName){
            if(refresh)
                evaluators.get(term).get(classNature).addRefresh();
            else if(renovate)
                evaluators.get(term).get(classNature).addRenovate();
        }
    }

    public void reduce(IDClassNameRenovate key,Iterable<ValueRecord> values,Context context) throws IOException, InterruptedException {
        String sid = key.getSid();
        Vector vector = getVector(sid,values);
        if(vector == null){
            context.getCounter(MissingRecord.NULL).increment(1);
        }
        else {
            String basePath = "";
            //这部分作类训练集.
            if(entryYearMap.get(sid).compareTo("2010") == 0){
                basePath = "train/gradeVector";
            }
            else if(entryYearMap.get(sid).compareTo("2009") == 0) //这部分作类测试集.这些都是可以更改的.
                basePath = "test/gradeVector";
            multipleOutputs.write(new Text(sid),new VectorWritable(vector),basePath);
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
