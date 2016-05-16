package analysize.record;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 5/4/16.
 * 将形成 studentID A(amount)
 *       studentID C(count)
 */
public class KeyPair implements WritableComparable<KeyPair>{

    /**
     * KeyPair的类型
     * A 代表 amount
     * C 代表 count
     ***/
    private String type;

    /**
     * 代表每个KeyPair的ID
     */
    private String ID;

    /**
     * 每个消费地点.
     */
    private String place;

    public KeyPair(){}

    public KeyPair(String type, String ID, String place){
        this.type = type;
        this.ID = ID;
        this.place = place;
    }

    public String getID(){
        return ID;
    }

    public String getType(){
        return type;
    }

    public String getPlace(){
        return place;
    }

    public int compareTo(KeyPair o) {

        return ID.compareTo(o.ID) == 0 ?
                place.compareTo(o.place) == 0 ?
                        type.compareTo(o.type) : place.compareTo(o.place):
                ID.compareTo(o.ID);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeUTF(ID);
        out.writeUTF(place);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = in.readUTF();
        this.ID = in.readUTF();
        this.place = in.readUTF();
    }

    public boolean equals(KeyPair o) {
        return compareTo(o) == 0 ? true : false;
    }

    public String toString(){
        return new StringBuilder().append(type).append("\t").append(ID).append("\t").append(place).toString();
    }

    /**
     * 需要注意,默认的partitioner是hashPartitioner.
     * (key.hashCode() & Integer.MaxValue) % numPartitioner;
     * 即每个key都应该有一个hashCode函数.
     * @return
     */
    public int hashCode(){
        return ID.hashCode() + place.hashCode() + type.hashCode() * 127;
    }


}