package dataBase;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by sghipr on 16-3-21.
 */
public class DB {

    private Properties properties;
    private String userName;
    private String passWord;
    private String url;

    public void setUrl(String url){
        this.url = url;
    }

    public void setUserName(String userName){
        this.userName = userName;
    }
    public void setPassWord(String passWord){
        this.passWord = passWord;
    }

    public DB(){
        properties = new Properties();
        try {
            properties.load((getClass().getResourceAsStream("/db.properties")));
        } catch (IOException e) {
            System.err.println("do not contains db.properties!");
            System.exit(1);
        }
        try {
            Class.forName(properties.getProperty("driverName"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        inital();
    }

    public DB(String db){
        properties = new Properties();
        try {
            properties.load(new FileInputStream(db));
        } catch (IOException e) {
            System.err.println("do not contains " + db);
            System.exit(1);
        }
        try {
            Class.forName(properties.getProperty("driverName"));
        } catch (ClassNotFoundException e) {
            System.err.println(e);
            System.exit(1);
        }
        inital();
    }

    public void inital(){
        userName = properties.getProperty("userName");
        url = properties.getProperty("url");
        passWord = properties.getProperty("passWord");
    }

    public Connection getConnection(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url,userName,passWord);
        } catch (SQLException e) {
            System.err.println(e);
            System.exit(1);
        }
        return conn;
    }
}
