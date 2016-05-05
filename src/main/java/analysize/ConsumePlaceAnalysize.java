package analysize;

import analysize.record.KeyPair;
import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Days;

/**
 * Created by sghipr on 5/4/16.
 * 分析每个学生在不同消费地点的平均每天的消费金额.
 *
 * 注意，在实际生产环境中任何一种数据功能都应该以一种流水线地方式来进行处理.
 * 不仅需要针对训练数据集，还需要针对预测数据集.
 *
 * 主要思路就是:
 * <studentID,place,type> AmountDayVector
 * <studentID,place,type> CountDayVector
 *
 * AmountDayVector/CountDayVector = 每个学生在每个地点平均每天的消费金额.
 * **/
public class ConsumePlaceAnalysize {
    /**
     *分析某个年级的学生某个时间段内的不同地点的消费情况.
     */
//    private static String rankYear;
//    private static String startTime;
//    private static String endTime;
    private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
//
//    public ConsumePlaceAnalysize(String rank, String start, String end){
//        startTime = start;
//        endTime = end;
//        rankYear = rank;
//    }

    public static class consumePlaceAnalysizeMapper extends Mapper<LongWritable,Text,KeyPair, VectorWritable>{

        /**
         * 在不同消费地点处的消费金额的Vector，初始化为0
         */
        //private HashMap<String,Integer> placeMap;
        private int days;
        private String startTime;
        private String endTime;
        private String rankYear;

        public void setup(Context context) throws IOException {
            startTime = context.getConfiguration().get("startTime");
            endTime = context.getConfiguration().get("endTime");
            rankYear = context.getConfiguration().get("rankYear");
            days = days(startTime, endTime);
        }
        public int days(String startTime,String endTime){
            int day = 0;
            try {
                day =  Days.daysBetween(new DateTime(df.parse(startTime)), new DateTime(df.parse(endTime))).getDays();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return day;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            GraduateStudentsConsumeAndBasicInfoRecord gscbr = new GraduateStudentsConsumeAndBasicInfoRecord(value.toString());
            if(gscbr.getStudentID().startsWith(rankYear) && gscbr.getTime().compareTo(startTime) >= 0 && gscbr.getTime().compareTo(endTime) <= 0){
                int day = days(startTime,gscbr.getTime().substring(0,8));
                Vector amountVector = new RandomAccessSparseVector(days + 1);
                Vector countVector = new RandomAccessSparseVector(days + 1);

                //amount
                amountVector.set(day, gscbr.getAmount());
                KeyPair amountKeyPair = new KeyPair("Amount",gscbr.getStudentID(),gscbr.getKind());

                //count
                //注意，这里countVector有个注意事项，即每个分量对应的是在这天消费的次数.一天可能会消费很多次.
                countVector.set(day, 1.0);
                KeyPair countKeyPair = new KeyPair("Count",gscbr.getStudentID(),gscbr.getKind());

                context.write(amountKeyPair,new VectorWritable(amountVector));
                context.write(countKeyPair, new VectorWritable(countVector));
            }
        }
    }


    public static class ConsumePlaceAnalysizeReduce extends Reducer<KeyPair, VectorWritable, KeyPair, VectorWritable>{

        public void reduce(KeyPair key,Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {

            Iterator<VectorWritable> iterator = values.iterator();
            Vector vector = new RandomAccessSparseVector(iterator.next().get());
            while(iterator.hasNext()){
                vector = vector.plus(iterator.next().get());
            }
            context.write(key,new VectorWritable(vector));
        }


    }

}
