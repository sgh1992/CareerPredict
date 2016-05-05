package analysize;

import analysize.record.KeyPair;
import dataProcess.consume.record.Key;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by sghipr on 5/5/16.
 * 将根据前述所计算的amountVector与countVector来及对应的PlaceMap来获得其所对应的
 * StudentID {place1:amountPerDay,place2:amountPerDay,...}
 */
public class ConsumePlaceAnalysizeCombine {

    public static class ConsumePlaceAnalysizeCombineReducer extends Reducer<KeyPair,VectorWritable, Text,VectorWritable>{

        private HashMap<String,Integer> placeMap;

        public void setup(Context context) throws IOException {
            placeMap = getPlaceMap(context);
        }

        public HashMap<String, Integer> getPlaceMap(Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(context.getCacheFiles()[0]))));
            String str = null;
            int size = 0;
            /**
             * 获得每个地点的映射编号.
             */
            HashMap<String, Integer> placeMap = new HashMap<>();
            while((str = reader.readLine()) != null){
                String[] array = str.split("\t", -1);
                placeMap.put(array[0], Integer.parseInt(array[1]));
                size++;
            }
            reader.close();
            return placeMap;
        }

        public void reduce(KeyPair keyPair,Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {

            Vector placeVector = new DenseVector(placeMap.size());
            int count = 0;
            double before = -1;
            for(VectorWritable vectorWritable : values){
                count++;
                if(count % 2 == 0)
                    placeVector.set(placeMap.get(keyPair.getPlace()),vectorWritable.get().zSum()/before);
                before = vectorWritable.get().zSum();
            }
            context.write(new Text(keyPair.getID()),new VectorWritable(placeVector));
        }
    }

    /**
     * 自定义partition操作.
     *
     */
    public static class ConsumePlaceAnalysizeCombinePartition extends Partitioner<KeyPair,VectorWritable>{

        @Override
        public int getPartition(KeyPair keyPair, VectorWritable vectorWritable, int numPartitions) {
            return Math.abs(keyPair.getID().hashCode() * 127 + keyPair.getPlace().hashCode()) % numPartitions;
        }
    }

    /**
     * 自定义group操作.
     * 以keyPair中的ID与place作为其分组行为.
     */
    public static class ConsumePlaceAnalysizeCombineGroup extends WritableComparator{

        public ConsumePlaceAnalysizeCombineGroup(){
            super(KeyPair.class, true);
        }

        public int compare(WritableComparable o1, WritableComparable o2){

            KeyPair pair1 = (KeyPair)o1;
            KeyPair pair2 = (KeyPair)o2;

            return pair1.getID().compareTo(pair2.getID()) == 0 ?
                    pair1.getPlace().compareTo(pair2.getPlace()):pair1.getID().compareTo(pair2.getID());
        }
    }

    /**
     * 自定义排序操作.
     */
    public static class ConsumePlaceAnalysizeCombineSort extends WritableComparator{

        public ConsumePlaceAnalysizeCombineSort(){
            super(KeyPair.class, true);
        }

        public int compare(WritableComparable o1, WritableComparable o2){
            return ((KeyPair)o1).compareTo((KeyPair)o2);
        }
    }


}
