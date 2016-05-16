package analysize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sghipr on 5/12/16.
 */
public class ConsumePlaceVetorMerge {

    public static class ConsumePlaceVectorMergeReducer extends Reducer<Text, VectorWritable, Text, VectorWritable>{
        public void reduce(Text studentID, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<VectorWritable> iterator = values.iterator();
            Vector mergeVector = new DenseVector(iterator.next().get());
            while (iterator.hasNext()){
                mergeVector = mergeVector.plus(iterator.next().get());
            }
            context.write(studentID,new VectorWritable(mergeVector));
        }
    }
}
