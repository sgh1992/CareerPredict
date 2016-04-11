package dataProcess.consume;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 4/11/16.
 */
public class Record implements Writable{
    protected String place;
    protected String deviceID;
    protected String time;
    protected double amount;
    protected double balance;

    public Record(){}

    public Record(String place, String deviceID, String time, double amount, double balance){
        this.place = place;
        this.deviceID = deviceID;
        this.time = time;
        this.amount = amount;
        this.balance = balance;
    }

    public Record(Record o){
        this(o.place,o.deviceID,o.time,o.amount,o.balance);
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