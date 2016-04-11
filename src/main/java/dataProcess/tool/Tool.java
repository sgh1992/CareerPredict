package dataProcess.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by sghipr on 11/04/16.
 */
public class Tool {
    /**
     * 毕业学生的基本信息的整条记录.
     */
   static class BasicRecord{
        String studentID;
        String year;
        String nation;
        String gender;
        String political;
        String college;
        String major;
        String birth;
        String work;
        String workunit;//工作单位.
        String workplace;//工作地点.
        String sector; //工作性质.
        String origin; //家庭省份.
        boolean validate = true;
        public BasicRecord(String line){
            String[] array = line.split(",", -1);
            if(array.length < 13) {
                validate = false;
                return;
            }
            year = array[0];
            studentID = array[1];
            nation = array[2];
            gender = array[3];
            political = array[4];
            birth = array[5];
            major = array[6];
            work = array[7];
            workunit = array[8];
            workplace = array[9];
            sector = array[10];
            origin = array[11];
            college = array[12];
        }
        public boolean isValidate(){
            return validate;
        }
    }
    private static Configuration conf;
    public Tool(Configuration conf){
        this.conf = conf;
    }

    /**
     * 获得毕业学生整体地基本信息,并存入到hdfs中.
     * @param file
     * @return
     */
    public static Path getGraduateStudentsBasicPath(String file){
        Path path = new Path(file);
        try {
            FSDataInputStream inputStream = FileSystem.get(conf).open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf),conf,path, NullWritable.class,GraduateStudentBasicRecord.class);
            while((line = reader.readLine()) != null){
                BasicRecord basicRecord = new BasicRecord(line);
                if(basicRecord.isValidate()){
                    GraduateStudentBasicRecord gRcord = new GraduateStudentBasicRecord(basicRecord.studentID,basicRecord.gender,basicRecord.nation,basicRecord.political,basicRecord.college,basicRecord.major,basicRecord.work);
                    writer.append(NullWritable.get(),gRcord);
                }
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return path;
    }
}
