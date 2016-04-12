package dataProcess.tool;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用来记录消费地点地映射记录.
 */
public class PlaceRecord implements Writable {

    /**
     * 原始消费地点.
     */
    private String origialPlace;

    /**
     * 经过某种清洗之后的消费地点.
     */
    private String transferPlace;

    /**
     * 初始划分的消费种类.
     */
    private String kind;

    /**
     * 消费场所.
     */
    private String position;

    public PlaceRecord(String origialPlace, String transferPlace,String kind, String position){
        this.origialPlace = origialPlace;
        this.transferPlace = transferPlace;
        this.kind = kind;
        this.position = position;
    }

    public PlaceRecord(){}

    public PlaceRecord(PlaceRecord o){
        this.origialPlace = o.origialPlace;
        this.transferPlace = o.transferPlace;
        this.kind = o.kind;
        this.position = o.position;
    }

    public String getOrigialPlace(){
        return origialPlace;
    }

    public String getTransferPlace(){
        return transferPlace;
    }

    public String getKind(){
        return kind;
    }

    public String getPosition(){
        return position;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(origialPlace);
        out.writeUTF(transferPlace);
        out.writeUTF(kind);
        out.writeUTF(position);
    }

    public void readFields(DataInput in) throws IOException {
        this.origialPlace = in.readUTF();
        this.transferPlace = in.readUTF();
        this.kind = in.readUTF();
        this.position = in.readUTF();
    }

    public String toString(){
        return new StringBuilder().append(origialPlace).append(",").append(transferPlace).append(",")
                .append(kind).append(",").append(position).toString();
    }
}