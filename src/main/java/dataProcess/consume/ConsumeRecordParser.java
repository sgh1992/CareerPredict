package dataProcess.consume;

/**
 * Created by sghipr on 4/8/16.
 * 解析每条记录.
 */
public class ConsumeRecordParser {

    protected String studentID;
    protected String place;
    protected String deviceID;
    protected String date;
    protected String time;
    protected double amount;
    protected double balance;
    private boolean head = false;

    public void parser(String line){
        String[] array = line.split(",", -1);
        studentID = array[1].trim();
        place = array[3].trim();
        deviceID = array[4].trim();
        date = array[5].trim();
        time = array[6].trim();
        try {
            amount = Double.parseDouble(array[7]);
            balance = Double.parseDouble(array[8]);
            head = false;
        }catch (NumberFormatException e){
            head = true;
        }
    }

    public boolean missingPlace(){
        if(place == null ||place.equals("null") || place.length() == 0)
            return true;
        return false;
    }

    public boolean isHead(){
        return head;
    }


}
