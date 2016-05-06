package analysize;

import dataProcess.consume.record.GraduateStudentsConsumeAndBasicInfoRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by sghipr on 5/5/16.
 *
 *主要是用来遍历整个文件找到其唯一的kind值.
 * 这里使用一个单独的线程来运行.
 * 其实这里并一定需要
 */
public class UniqueKind implements Callable<Path>{

    private static String consumePlace = "consumePlace";

    private Path graduateStudentsForConsumeAndBasicInfo;
    private Configuration conf;

    public UniqueKind(Path graduateStudentsForConsumeAndBasicInfo, Configuration conf){
        this.graduateStudentsForConsumeAndBasicInfo = graduateStudentsForConsumeAndBasicInfo;
        this.conf = conf;
    }

    public void paths(Path path,List<Path> list) throws IOException {

        FileSystem fileSystem = FileSystem.get(conf);
        for(FileStatus status : fileSystem.listStatus(path)){
            if(status.isDirectory())
               paths(status.getPath(),list);
            else
                list.add(status.getPath());
        }
    }

    public Path writeConsumePlace(HashMap<String,Integer> placeMap) throws IOException {

        Path output = new Path(graduateStudentsForConsumeAndBasicInfo.getParent(),consumePlace);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(FileSystem.get(conf).create(output)));
        //SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(conf),conf,output, Text.class, IntWritable.class);
        for(Map.Entry<String, Integer> entry : placeMap.entrySet()){
            writer.write(entry.getKey() + "\t" + entry.getValue());
            writer.newLine();
        }
        writer.close();
        return output;
    }

    @Override
    public Path call() throws Exception {
        List<Path> listPaths = new ArrayList<>();
        paths(graduateStudentsForConsumeAndBasicInfo,listPaths);
        HashMap<String, Integer> placeMap = new HashMap<>();
        try {
            BufferedReader reader = null;
            for(Path path : listPaths){
                reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(path)));
                String str = null;
                while((str = reader.readLine()) != null){
                    GraduateStudentsConsumeAndBasicInfoRecord record = new GraduateStudentsConsumeAndBasicInfoRecord(str);
                    if(!placeMap.containsKey(record.getKind()))
                        placeMap.put(record.getKind(),placeMap.size());
                }
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return writeConsumePlace(placeMap);
    }
}