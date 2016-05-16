package analysize;

import analysize.record.KeyPair;
import dataProcess.consume.record.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

        public void listPaths(Configuration conf,Path path, List<Path> pathList) throws IOException {
            for(FileStatus status : FileSystem.get(conf).listStatus(path)){
                if(status.isDirectory())
                    listPaths(conf,status.getPath(),pathList);
                else
                    pathList.add(status.getPath());
            }
        }

        public HashMap<String, Integer> getPlaceMap(Context context) throws IOException {

            List<Path> paths = new ArrayList<>();
            listPaths(context.getConfiguration(), new Path(context.getCacheFiles()[0]), paths);
            BufferedReader reader = null;
            /**
             * 获得每个地点的映射编号.
             */
            HashMap<String, Integer> placeMap = new HashMap<>();
            for(Path path : paths){
                reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(path)));
                String str = null;
                //file text is a Set
                while((str = reader.readLine()) != null){
                    placeMap.put(str.trim(), placeMap.size());
                }
                reader.close();
            }
            return placeMap;
        }

        /**
         * 这里有个疑问,即经过分组之后的key，并不仅仅只有唯一的key值.每次对values进行迭代之后，它的key值都将随之改变.
         * @param keyPair
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(KeyPair keyPair,Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {

            Vector placeVector = new DenseVector(placeMap.size());
            int count = 0;
            double before = -1;
            for(VectorWritable vectorWritable : values){
                count++;
                if(count % 2 == 0)
                    placeVector.set(placeMap.get(keyPair.getPlace()),before/vectorWritable.get().zSum());// amount/days
                before = vectorWritable.get().zSum();
            }
            context.write(new Text(keyPair.getID()),new VectorWritable(placeVector));
        }
    }

    /**
     * 自定义partition操作.
     *以KeyPair中的ID作为其partition的依据.
     */
    public static class ConsumePlaceAnalysizeCombinePartition extends Partitioner<KeyPair,VectorWritable>{
        @Override
        public int getPartition(KeyPair keyPair, VectorWritable vectorWritable, int numPartitions) {
            return Math.abs(keyPair.getID().hashCode() * 127) % numPartitions;
        }
    }

    /**
     * 自定义group操作.
     * 以keyPair中的ID作为其分组行为.
     * 因为最终得到的是每一个学生的所有地点的Vector，这点要非常注意.
     */
    public static class ConsumePlaceAnalysizeCombineGroup extends WritableComparator{

        public ConsumePlaceAnalysizeCombineGroup(){
            super(KeyPair.class, true);
        }

        public int compare(WritableComparable o1, WritableComparable o2){
            KeyPair pair1 = (KeyPair)o1;
            KeyPair pair2 = (KeyPair)o2;
            return pair1.getID().compareTo(pair2.getID());
        }
    }

    /**
     * 自定义Sort操作.
     * 分别以ID,Place,type顺序来进行排序.
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
