package uk.ac.ebi.fgpt.sampletab;
import junit.framework.TestCase;
import uk.ac.ebi.fgpt.sampletab.utils.SubsTracking;
import java.sql.*;

public class TestSubsTracking extends TestCase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url = "jdbc:mysql://mysql-fg-biosamples-submission.ebi.ac.uk:4421/"; // //10.7.49.7
        String dbName = "biosd_autosubs_dev";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "admin";
        String password = "c67YPIpb";
        try {
            Class.forName(driver).newInstance();
            String connectionString = url+dbName+"?user="+userName+"&password="+password+"&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
            //Connection conn = DriverManager.getConnection(url+dbName,userName,password);
            System.out.println("Connection  string is :" + connectionString);
            Connection conn = DriverManager.getConnection(connectionString);
            System.out.println("Connection established successfully");
            conn.close();
            } catch (Exception e) {
            e.printStackTrace();
            }
            }



	}

