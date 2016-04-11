package dataProcess.consume;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by sghipr on 4/8/16.
 * 消费数据中有部分重复的数据
 * 运用MapReduce来进行去重.
 */
public class Deduplication {

    static class KeyPartition extends Partitioner<Key,Record>{
        public int getPartition(Key key, Record record, int numPartitions) {
            return Math.abs(key.studentID.hashCode() * 127) % numPartitions;
        }
    }

    static class KeyGroupCompartor extends WritableComparator{
        public KeyGroupCompartor(){
            super(Key.class,true);
        }
        public int compare(WritableComparable key1, WritableComparable key2){
            Key first = (Key)key1;
            Key second = (Key)key2;
            return first.studentID.compareTo(second.studentID);
        }
    }

    static enum MISSING{
        PLACEMISSING,
        RECORDMISSING;
    }

    static class DeMapper extends Mapper<LongWritable,Text,Key,Record>{

        private ConsumeRecordParser parser;

        public void setup(Context context){
            parser = new ConsumeRecordParser();
        }

        /**
         * 需要特别注意,Map阶段,一次只有一条记录在内存中,而且一个分片中的多条记录是共享一个实例对象的
         * 所以当在外部引用了一Key or Value 实例时，在后续的操作中，随着分片记录地进行,Key or Value中的值是会发生变化的.
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            if(parser.unMatched()) {
                context.getCounter(MISSING.RECORDMISSING).increment(1);
                return;
            }
            if(parser.missingPlace())
                context.getCounter(MISSING.PLACEMISSING).increment(1);
            if(!parser.isHead())
                context.write(getKey(),getRecord());
        }

        public Key getKey(){
            String studentID = parser.studentID;
            String time = new StringBuilder().append(parser.date).append(" ").append(parser.time).toString();
            return new Key(studentID,time);
        }

        public Record getRecord(){
            return new Record(parser.place,parser.deviceID,new StringBuilder().append(parser.date).append(" ").append(parser.time).toString(),parser.amount,parser.balance);
        }
    }

    static class DeReduce extends Reducer<Key,Record,Text,Text>{
        /**
         * Iterable中的所有对象共享一个实例,这点非常值得注意
         * 因此，只要确保单个的Key Value 不会超过内存空间的大小即可.
         * @param key
         * @param iterable
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Key key, Iterable<Record> iterable, Context context) throws IOException, InterruptedException {
            String studentID = key.studentID;
            Iterator<Record> iterator = iterable.iterator();
            /**
             * 注意，这里需要深度复制.
             */
            Record beforeRecord = new Record(iterator.next());
            /**
             * 根据同一个ID，同一个时间点的策略消除重复的记录.
             */
            while(iterator.hasNext()){
                Record curRecord = iterator.next();
                if(!beforeRecord.time.equals(curRecord.time))
                    context.write(new Text(studentID), new Text(beforeRecord.toString()));
                beforeRecord.time = curRecord.time;
            }
            context.write(new Text(studentID), new Text(beforeRecord.toString()));
        }
    }



}
