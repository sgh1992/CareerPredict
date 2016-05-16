package dataProcess.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.*;

/**
 * Created by sghipr on 11/04/16.
 */
public class Tool {


    /**
     * 毕业学生的基本信息数据.
     */
    private static String graduateStudentsSequenceFile = "graduateStudentsBasicSequenceFile";

    /**
     * 将毕业学生的离散型属性进行dummy code.将之转换为0,1变量.
     */
    private static String graduateStudentsBasicMap = "graduateStudentsBasicMap";

    /**
     * 消费地点的划分
     */
    private static String ConsumePlaceTransfer = "consumePlaceTransfer";

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


        /**
         * 返回学生的 nation,gender,political,major,college 的信息.
         * 即在校时的个人属性信息
         * @return
         */
        public Iterable<String> personalInfo(){
            List<String> infos = new ArrayList<>();
            infos.add(nation);
            infos.add(gender);
            infos.add(political);
            infos.add(major);
            infos.add(college);
            return infos;
        }
        public String toString(){

            return new StringBuilder().append(year).append(",").append(studentID).append(",").append(nation).append(",")
                    .append(gender).append(",").append(political).append(",").append(birth).append(",").append(major)
                    .append(",").append(work).append(",").append(workunit).append(",").append(workplace).append(",")
                    .append(sector).append(",").append(origin).append(",").append(college)
                    .toString();
        }


    }
    private  Configuration conf;
    public Tool(Configuration conf){
        this.conf = conf;
    }

    /**
     * 获得毕业学生整体地基本信息,并存入到hdfs中.
     * @param file
     * @return
     */
    public Path getGraduateStudentsBasicPath(String file){
        Path path = new Path(file);
        Path output = new Path(path.getParent(),graduateStudentsSequenceFile);
        try {
            FSDataInputStream inputStream = FileSystem.get(conf).open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf),conf,output, NullWritable.class,GraduateStudentBasicRecord.class);
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
        return output;
    }

    /**
     * 毕业学生的基本信息映射.
     * 主要用于dummy code.
     * @param workinfoFile
     * @return
     * @throws IOException
     */
    public Path graduateStudentsBasicMap(String workinfoFile) throws IOException {
        HashMap<String, Integer> basicMap = studentBasicMap(workinfoFile);
        Path basicMapPath = new Path(new Path(workinfoFile).getParent(),graduateStudentsBasicMap);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(FileSystem.get(conf).create(basicMapPath)));
        for(Map.Entry<String, Integer> entry : basicMap.entrySet()){
            writer.write(entry.getKey() + "\t" + entry.getValue());
            writer.newLine();
        }
        writer.close();
        return basicMapPath;
    }

    public HashMap<String, Integer> studentBasicMap(String workInfoFile) throws IOException {
        Path input = new Path(workInfoFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(input)));
        String str = null;
        HashMap<String,Integer> basicMap = new HashMap<>();
        while((str = reader.readLine()) != null){
            BasicRecord basicRecord = new BasicRecord(str);
            if(basicRecord.isValidate()){
                for(String info : basicRecord.personalInfo()){
                    if(!basicMap.containsKey(info))
                        basicMap.put(info, basicMap.size());
                }
            }
        }
        reader.close();
        return basicMap;
    }

    /**
     * 将消费数据的地址转换为一系列可以理解的数据，并初步对消费地点进行分组.
     * @param placeFile
     * @return
     */
    public Path consumePlaceTransfer(String placeFile){

        Path input = new Path(placeFile);
        Path output = new Path(input.getParent(),ConsumePlaceTransfer);
        try {
            FSDataInputStream inputStream = FileSystem.get(conf).open(input);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf),conf,output,NullWritable.class,PlaceRecord.class);
            String str = null;
            while((str = reader.readLine()) != null){
                String[] array = placeRule(str);
                writer.append(NullWritable.get(),new PlaceRecord(str,array[0],array[1],array[2]));
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return output;
    }

    private String[] placeRule(String place){
        String[] transferAndClass = new String[3];
        if(place.contains("食堂") || place.contains("二食2F66#000A547A")){
            if(place.contains("吧台")){
                transferAndClass[0] = place.substring(0,place.indexOf('台') + 1);
                transferAndClass[1] = "snack";//食堂小吃.
                transferAndClass[2] = "mess";
            }
            else if(place.contains("店")){
                transferAndClass[0] = place.substring(0,place.indexOf('店') + 1);
                transferAndClass[1] = "fruit";
                transferAndClass[2] = "mess";
            }
            else {
                if(place.equals("二食2F66#000A547A"))
                    place = "二食堂2F";
                if(!place.contains("F")){
                    System.err.println("Missing in 食堂\t" + place);
                    transferAndClass[0] = place;
                }
                else
                    transferAndClass[0] = place.substring(0, place.indexOf('F') + 1);
                transferAndClass[1] = "mess";
                transferAndClass[2] = "mess";
            }
        }

        else if(place.contains("餐厅")){

            if(place.contains("吧台")){
                transferAndClass[0] = place.substring(0,place.indexOf('台') + 1);
                transferAndClass[1] = "snack";
                transferAndClass[2] = "mess";
            }
            else {
                transferAndClass[0] = place.substring(0, place.indexOf('厅') + 1);
                transferAndClass[1] = "mess";
                transferAndClass[2] = "mess";
            }
        }

        else if(place.contains("超市")){
            transferAndClass[0] = place.substring(0,place.indexOf('市') + 1);
            if(place.contains("水果"))
                transferAndClass[1] = "fruit";
            else
                transferAndClass[1] = "supermarket";
            transferAndClass[2] = "supermarket";
        }

        else if(place.contains("教学楼")){
            if(place.contains("开"))
                transferAndClass[0] = place.substring(0,place.indexOf('开'));
            else {
                transferAndClass[0] = place;
                System.err.println("Missing in 教学楼\t" + place);
            }
            transferAndClass[1] = "water";
            transferAndClass[2] = "teachingBuilding";
        }
        else if(place.contains("图书馆")){
            if(place.contains("电子阅览室")){
                transferAndClass[0] = place.substring(0,place.indexOf('室') + 1);
                transferAndClass[1] = "ElectronicReading";
                transferAndClass[2] = "library";
            }
            else if(place.contains("水吧")){
                transferAndClass[0] = place.substring(0,place.indexOf('吧') + 1);
                transferAndClass[1] = "snack";
                transferAndClass[2] = "library";
            }
            else if(place.contains("复印机")){
                transferAndClass[0] = place.substring(0,place.indexOf('机') + 1);
                transferAndClass[1] = "print";
                transferAndClass[2] = "library";
            }
            else if(place.contains("收费机") || place.contains("罚款")){
                if(place.contains("收费机"))
                    transferAndClass[0] = place.substring(0,place.indexOf('机') + 1);
                else
                    transferAndClass[0] = place.substring(0,place.indexOf('款') + 1);

                transferAndClass[1] = "BorrowingCost";
                transferAndClass[2] = "library";
            }
            else if(place.contains("放映厅")){
                transferAndClass[0] = place.substring(0,place.indexOf('厅') + 1);
                transferAndClass[1] = "movie";
                transferAndClass[2] = "leisure";
            }
            else{
                transferAndClass[0] = place;
                transferAndClass[1] = "others";
                transferAndClass[2] = "library";
            }

        }
        else if(place.contains("本")){
            if(place.contains("开")){
                transferAndClass[0] = place.substring(0,place.indexOf('开'));
                transferAndClass[1] = "water";
                transferAndClass[2] = "dormitory";
            }
            else if(place.contains("洗")){
                transferAndClass[0] = place.substring(0,place.indexOf('洗'));
                transferAndClass[1] = "cleaning";
                transferAndClass[2] = "dormitory";
            }
            else if(place.contains("淋")){
                transferAndClass[0] = place.substring(0,place.indexOf('淋'));
                transferAndClass[1] = "bathe";
                transferAndClass[2] = "dormitory";
            }
            else if(place.contains("浴")){
                transferAndClass[0] = place.substring(0,place.indexOf('浴'));
                transferAndClass[1] = "bathe";
                transferAndClass[2] = "dormitory";
            }
            else if(place.contains("黑名单")){
                transferAndClass[0] = "黑名单";
                transferAndClass[1] = "leisure";
                transferAndClass[2] = "dormitory";
            }
            else{
                transferAndClass[0] = place;
                transferAndClass[1] = "others";
                transferAndClass[2] = "dormitory";
            }
        }
        else if(place.contains("欣村")){

            if(place.contains(" ")){
                String[] array = place.split(" ", -1);
                transferAndClass[0] = array[0];
                if(array.length < 3){
                    //System.err.println("Missing in 欣村\t" + place);
                    if(array[1].contains("开"))
                        transferAndClass[1] = "water";
                    transferAndClass[2] = "dormitory";
                    return transferAndClass;
                }
                if(array[2].contains("开")){
                    transferAndClass[1] = "water";
                }
                else if(array[2].contains("洗")){
                    transferAndClass[1] = "cleaning";
                }
                else if(array[2].contains("淋"))
                    transferAndClass[1] = "bathe";

                else if(array[2].contains("浴"))
                    transferAndClass[1] = "bathe";
                else{
                    System.err.println("Missing in 欣村\t" + place);
                    transferAndClass[1] = "others";
                }
            }
            else{
                if(place.contains("开")) {
                    transferAndClass[0] = place.substring(0,place.indexOf('开'));
                    transferAndClass[1] = "water";
                }
                else if(place.contains("洗")) {
                    transferAndClass[0] = place.substring(0,place.indexOf('洗'));
                    transferAndClass[1] = "cleaning";
                }
                else if(place.contains("淋")) {
                    transferAndClass[0] = place.substring(0,place.indexOf('淋'));
                    transferAndClass[1] = "bathe";
                }
                else if(place.contains("浴")) {
                    transferAndClass[0] = place.substring(0,place.indexOf('浴'));
                    transferAndClass[1] = "bathe";
                }
                else{
                    System.err.println("Missing in 欣村\t" + place);
                    transferAndClass[1] = "others";
                }
            }
            transferAndClass[2] = "dormitory";
        }

        else if(place.contains("新村")){
            if(place.contains("开")){
                if(place.contains("新增"))
                    transferAndClass[0] = place.substring(0,place.indexOf('新'));
                else
                    transferAndClass[0] = place.substring(0,place.indexOf('开'));
                transferAndClass[1] = "water";
            }
            else if(place.contains("洗")){
                if(place.contains("新增"))
                    transferAndClass[0] = place.substring(0,place.indexOf('新'));
                else
                    transferAndClass[0] = place.substring(0,place.indexOf('洗'));
                transferAndClass[1] = "cleaning";
            }
            else if(place.contains("浴")) {
                transferAndClass[0] = place.substring(0, place.indexOf('浴'));
                transferAndClass[1] = "bathe";
            }
            else{
                System.err.println("Missing in 新村\t" + place);
                transferAndClass[0] = place;
                transferAndClass[1] = "others";
            }
            transferAndClass[2] = "dormitory";
        }

        else if(place.contains("百味屋") || place.contains("上海周记") || place.contains("校园二分店")
                || place.contains("硕士20栋校园6店")){
            if(place.contains("百味屋"))
                transferAndClass[0] = "百味屋";
            else if(place.contains("上海周记"))
                transferAndClass[0] = "上海周记";
            else if(place.contains("校园二分店"))
                transferAndClass[0] = "校园二分店";
            else
                transferAndClass[0] = "硕士20栋校园6店";
            transferAndClass[1] = "supermarket";
            transferAndClass[2] = "supermarket";
        }

        else if(place.startsWith("硕")){

            if(place.contains("开")) {
                transferAndClass[0] = place.substring(0, place.indexOf('开'));
                transferAndClass[1] = "water";
            }
            else if(place.contains("洗")) {
                transferAndClass[0] = place.substring(0, place.indexOf('洗'));
                transferAndClass[1] = "cleaning";
            }
            else if(place.contains("淋")) {
                transferAndClass[0] = place.substring(0, place.indexOf('淋'));
                transferAndClass[1] = "bathe";
            }
            else{
                System.err.println("Missing in 硕\t" + place);
                transferAndClass[0] = place;
                transferAndClass[1] = "others";
            }
            transferAndClass[2] = "dormitory";
        }
        else if(place.contains("成电班车") || place.contains("公交车载")){
            transferAndClass[0] = "成电班车";
            transferAndClass[1] = "driverOutSide";
            transferAndClass[2] = "driver";
        }
        else if(place.contains("成电车载")){
            transferAndClass[0] = "校内直通车";
            transferAndClass[1] = "driverInSide";
            transferAndClass[2] = "driver";
        }
        else if(place.contains("文印")){
            transferAndClass[0] = "校内文印";
            transferAndClass[1] = "print";
            transferAndClass[2] = "print";
        }
        else if(place.contains("水果店")){
            transferAndClass[0] = place.substring(0,place.indexOf('店') + 1);
            transferAndClass[1] = "fruit";
            transferAndClass[2] = "supermarket";
        }
        else if(place.contains("教务处读卡器收费")){
            transferAndClass[0] = "教务处读卡器收费";
            transferAndClass[1] = "learning";
            transferAndClass[2] = "learning";
        }
        else if(place.contains("主楼档案馆")){
            transferAndClass[0] = "主楼档案馆";
            transferAndClass[1] = "learning";
            transferAndClass[2] = "learning";
        }
        else if(place.contains("中间设备") || place.contains("卡库不平处理设备")){
            transferAndClass[0] = "设备";
            transferAndClass[1] = "device";
            transferAndClass[2] = "others";
        }
        else if(place.contains("清水河博士楼工程服务部IP机POS3")){
            transferAndClass[0] = "清水河博士楼工程服务部";
            transferAndClass[1] = "device";
            transferAndClass[2] = "others";
        }
        else if(place.contains("留学生") || place.contains("博")){
            if(place.contains("开")) {
                transferAndClass[0] = place.substring(0, place.indexOf('开') + 1);
                transferAndClass[1] = "water";
            }
            else if(place.contains("洗")) {
                transferAndClass[0] = place.substring(0, place.indexOf('洗') + 1);
                transferAndClass[1] = "cleaning";
            }
            else if(place.contains("淋")) {
                transferAndClass[0] = place.substring(0, place.indexOf('淋') + 1);
                transferAndClass[1] = "bathe";
            }
            else{
                System.err.println("Missing in 留学生\t" + place);
                transferAndClass[0] = place;
                transferAndClass[1] = "others";
            }
            transferAndClass[2] = "dormitory";
        }
        else if(place.contains("咖啡") || place.contains("茶楼")){
            if(place.contains("咖啡")) {
                transferAndClass[0] = place.substring(0, place.indexOf('啡') + 1);
                transferAndClass[1] = "leisure";
            }
            else {
                transferAndClass[0] = place.substring(0, place.indexOf('楼') + 1);
                transferAndClass[1] = "leisure";
            }
            transferAndClass[2] = "leisure";
        }
        else if(place.contains("电影院")){
            transferAndClass[0] = place.substring(0,place.indexOf('院') + 1);
            transferAndClass[1] = "movie";
            transferAndClass[2] = "leisure";
        }
        else if(place.contains("球场")){
            transferAndClass[0] = place.substring(0,place.indexOf('场') + 1);
            transferAndClass[1] = "sports";
            transferAndClass[2] = "stadium";//体育馆
        }
        else if(place.contains("游泳池")){
            transferAndClass[0] = place.substring(0,place.indexOf('池') + 1);
            transferAndClass[1] = "sports";
            transferAndClass[2] = "stadium";
        }
        else if(place.contains("沙河水站")){
            transferAndClass[0] = "沙河水站";
            transferAndClass[1] = "others";
            transferAndClass[2] = "others";
        }
        else if(place.contains("汇多")){
            transferAndClass[0] = "汇多";
            transferAndClass[1] = "supermarket";
            transferAndClass[2] = "supermarket";
        }

        else if(place.contains("医院")){
            transferAndClass[0] = place.substring(0,place.indexOf('院') + 1);
            transferAndClass[1] = "hospital";
            transferAndClass[2] = "hospital";
        }

        else if(place.contains("塞文斯休闲水吧")){
            transferAndClass[0] = "塞文斯休闲水吧";
            transferAndClass[1] = "leisure";
            transferAndClass[2] = "leisure";
        }

        else if(place.contains("null")){
            transferAndClass[0] = "missing";
            transferAndClass[1] = "missing";
            transferAndClass[2] = "missing";
        }
        else if(place.contains("5F淋064#000A9CBF")){
            transferAndClass[0] = "5F";
            transferAndClass[1] = "bathe";
            transferAndClass[2] = "dormitory";
        }
        else if(place.contains("deviceName")){
            transferAndClass[0] = "deviceName";
            transferAndClass[1] = "missing";
            transferAndClass[2] = "missing";
        }
        else if(place.contains("清水河加油站IP机1")){
            transferAndClass[0] = "加油站";
            transferAndClass[1] = "oil";
            transferAndClass[2] = "others";
        }
        else{
            transferAndClass[0] = place;
            transferAndClass[1] = "others";
            transferAndClass[2] = "others";
            System.err.println("Missing \t" + place);
        }
        return transferAndClass;
    }

    public void iteratePath(Path root, List<Path> paths) throws IOException {
        for(FileStatus status : FileSystem.get(conf).listStatus(root)){
            if(status.isDirectory())
                iteratePath(status.getPath() ,paths);
            else
                paths.add(status.getPath());
        }
    }

}
