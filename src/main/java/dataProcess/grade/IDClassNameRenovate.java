package dataProcess.grade;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 16-3-21.
 * 每个主键的记录.
 *
 */
public class IDClassNameRenovate implements WritableComparable<IDClassNameRenovate>{

    private String sid;
    private String className;
    private short renovate;

    public String getSid(){
        return sid;
    }

    public String getClassName(){
        return className;
    }

    public short getRenovate(){
        return renovate;
    }

    public IDClassNameRenovate(){}

    public IDClassNameRenovate(String sid, String className, short renovate){
        this.sid = sid;
        this.className = className;
        this.renovate = renovate;
    }

    /**
     * 分别以sid,className,renovate顺序进行排序.
     * @param o
     * @return
     */
    public int compareTo(IDClassNameRenovate o) {
        if(sid.equals(o.sid)){
            if(className.equals(o.className))
                return renovate - o.renovate;
            else
                return className.compareTo(o.className);
        }
        else{
            return sid.compareTo(o.sid);
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sid);
        out.writeUTF(className);
        out.writeShort(renovate);
    }

    public void readFields(DataInput in) throws IOException {
        sid = in.readUTF();
        className = in.readUTF();
        renovate = in.readShort();
    }

    public boolean equals(IDClassNameRenovate o){
        if(sid.equals(o.sid) && className.equals(o.className) && renovate == o.renovate)
            return true;
        return false;
    }
    public int hashCode(){
        return sid.hashCode() % 13 + className.hashCode() + renovate * 37;
    }
}
