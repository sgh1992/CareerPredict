package dataProcess.consume.record;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 4/11/16.
 * 消费数据的Record
 */
public class Record implements Writable {
    protected String place;
    protected String deviceID;
    protected String time;
    protected double amount;
    protected double balance;
    protected String type;

    public String getTime(){
        return  time;
    }

    public void setTime(String time){
        this.time = time;
    }

    public String getPlace(){
        return place;
    }

    public Record() {
    }

    public Record(String type,String place, String deviceID, String time, double amount, double balance) {

        this.type = type;
        this.place = place;
        this.deviceID = deviceID;
        this.time = time;
        this.amount = amount;
        this.balance = balance;
    }

    public Record(Record o) {
        this(o.type,o.place, o.deviceID, o.time, o.amount, o.balance);
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(type);
        dataOutput.writeUTF(place);
        dataOutput.writeUTF(deviceID);
        dataOutput.writeUTF(time);
        dataOutput.writeDouble(amount);
        dataOutput.writeDouble(balance);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.place = dataInput.readUTF();
        this.deviceID = dataInput.readUTF();
        this.time = dataInput.readUTF();
        this.amount = dataInput.readDouble();
        this.balance = dataInput.readDouble();
    }

    public String toString() {
        return new StringBuilder().append(type).append(",").append(place).append(",").append(deviceID).append(",").append(time).append(",").append(amount).append(",").append(balance).toString();
    }

    public Record(String toString){

        String[] array = toString.split(",", -1);
        type = array[0];
        place = array[1];
        deviceID = array[2];
        time = array[3];
        amount = Double.parseDouble(array[4]);
        balance = Double.parseDouble(array[5]);
    }
}
