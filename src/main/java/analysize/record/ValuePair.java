package analysize.record;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 5/4/16.
 * 用来存放对应的KeyPair所对应的ValuePair.
 *
 **/
public class ValuePair implements Writable{

    /**
     * 与keyPair的type是相对应的.
     */
    private String type;

    /**
     * 各个时间地点所对应的值.
     */
    private VectorWritable vector;

    /**
     * 只用于序列化时起作用.
     */
    public ValuePair(){}

    public ValuePair(String type, VectorWritable vector){
        this.type = type;
        this.vector = vector;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(type);
        vector.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = in.readUTF();
        this.vector.readFields(in);
    }

    public String toString(){
        return new StringBuilder().append(type).append("\t").append(vector.toString()).toString();
    }

}
