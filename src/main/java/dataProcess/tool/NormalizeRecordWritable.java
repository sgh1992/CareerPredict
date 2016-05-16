package dataProcess.tool;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sghipr on 5/13/16.
 */
public class NormalizeRecordWritable implements WritableComparable<NormalizeRecordWritable> {

    private int index;

    private double squareSum;

    private double sum;

    private int num;

    public NormalizeRecordWritable(){}

    public NormalizeRecordWritable(int index, double squareSum, double sum){
        this.index = index;
        this.sum = sum;
        this.squareSum = squareSum;
    }

    public int getIndex(){
        return index;
    }

    public double getSquareSum(){
        return squareSum;
    }

    public double getSum(){
        return sum;
    }

    public double getMean(){
        if(num != 0)
            return sum/num;
        return 0;
    }

    public double getStd(){
        if(num != 0) {
            if((squareSum * num - sum * sum) / (num * num) < 0)
                System.err.println("std less 0");
            return (squareSum * num - sum * sum) / (num * num);
        }
        return 0;
    }

    public int getNum(){
        return num;
    }

    public void addSquare(double x){
        squareSum += x * x;
    }

    public void addSum(double x){
        sum += x;
    }

    public void addNum(){
        this.addNum(1);
    }

    public void updateSum(double sum){
        this.sum += sum;
    }
    public void updateSquare(double square){
        this.squareSum += square;
    }

    public void addNum(int num){
        this.num += num;
    }

    @Override
    public int compareTo(NormalizeRecordWritable o) {
        return  index - o.index;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        out.writeDouble(sum);
        out.writeDouble(squareSum);
        out.writeInt(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.index = in.readInt();
        this.sum = in.readDouble();
        this.squareSum = in.readDouble();
        this.num = in.readInt();
    }
    public int hashCode(){
        return index;
    }

    public boolean equals(NormalizeRecordWritable o){
        return index == o.index;
    }
}
