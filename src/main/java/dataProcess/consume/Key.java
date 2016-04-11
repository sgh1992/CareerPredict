package dataProcess.consume;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 4/11/16.
 */
public class Key implements WritableComparable<Key>{
    protected String studentID;
    protected String time;

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

    public String toString(){
        return studentID + "," + time;
    }
}
