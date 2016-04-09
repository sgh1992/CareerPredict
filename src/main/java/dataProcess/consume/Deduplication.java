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

    static class Key implements WritableComparable<Key> {

        private String studentID;
        private String time;

        public Key(){}

        public Key(String studentID, String time){
            this.studentID = studentID;
            this.time = time;
        }

        public int compareTo(Key o) {
            if(studentID.compareTo(o.studentID) == 0)
                return time.compareTo(o.time);
            else
                return studentID.compareTo(o.studentID);
        }

        /**
         * 注意需要重写equals与 hashCode方法.
         * @param o
         * @return
         */
        public boolean equals(Key o){
            if(compareTo(o) == 0)
                return true;
            return false;
        }

        public int hashCode(){
            return (studentID.hashCode() * 127 + time.hashCode());
        }

        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeUTF(studentID);
            dataOutput.writeUTF(time);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.studentID = dataInput.readUTF();
            this.time = dataInput.readUTF();
        }

    }

    static class Record implements Writable{

        private String place;
        private String deviceID;
        private String time;
        private double amount;
        private double balance;

        public Record(){}

        public Record(String place, String deviceID, String time, double amount, double balance){
            this.place = place;
            this.deviceID = deviceID;
            this.time = time;
            this.amount = amount;
            this.balance = balance;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(place);
            dataOutput.writeUTF(deviceID);
            dataOutput.writeUTF(time);
            dataOutput.writeDouble(amount);
            dataOutput.writeDouble(balance);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.place = dataInput.readUTF();
            this.deviceID = dataInput.readUTF();
            this.time = dataInput.readUTF();
            this.amount = dataInput.readDouble();
            this.balance = dataInput.readDouble();
        }

        public String toString(){
            return new StringBuilder().append(place).append(",").append(deviceID).append(",").append(time).append(",").append(amount).append(",").append(balance).toString();
        }
    }

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

    static enum MISSINGPLACE{
        PLACEMISSING;
    }

    static class DeMapper extends Mapper<LongWritable,Text,Key,Record>{

        private ConsumeRecordParser parser;

        public void setup(Context context){
            parser = new ConsumeRecordParser();
        }

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            if(parser.missingPlace())
                context.getCounter(MISSINGPLACE.PLACEMISSING).increment(1);
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

        public void reduce(Key key, Iterable<Record> iterable, Context context) throws IOException, InterruptedException {
            String studentID = key.studentID;
            Iterator<Record> iterator = iterable.iterator();
            Record beforeRecord = iterator.next();
            /**
             * 根据同一个ID，同一个时间点的策略消除重复的记录.
             */
            while(iterator.hasNext()){
                Record curRecord = iterator.next();
                if(!beforeRecord.time.equals(curRecord.time)){
                    context.write(new Text(studentID), new Text(beforeRecord.toString()));
                }
                beforeRecord = curRecord;
            }
            context.write(new Text(studentID), new Text(beforeRecord.toString()));
        }
    }



}
