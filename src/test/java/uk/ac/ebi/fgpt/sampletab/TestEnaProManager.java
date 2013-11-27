package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import junit.framework.TestCase;

public class TestEnaProManager extends TestCase{
	
	private Logger log = LoggerFactory.getLogger(getClass());
	
	
	public void testConnection(){
		
		Properties properties = new Properties();
		try {
			InputStream is = getClass().getResourceAsStream(
					"/era-pro.properties");
			properties.load(is);
		} catch (IOException e) {
			log.error("Unable to read resource oracle.properties", e);
		}
		String hostname = properties.getProperty("hostname");
		Integer port = new Integer(properties.getProperty("port"));
		String database = properties.getProperty("database");
		String username = properties.getProperty("username");
		String password = properties.getProperty("password");

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			log.error("Unable to find oracle.jdbc.driver.OracleDriver");
		}

		String jdbc = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
		
		Connection conn = null;
		 
		
		try {
		conn = DriverManager.getConnection(jdbc, username, password);
		
		}catch(Exception e){
		
		fail();
		
		} 
		
		try{
			Statement stmt = conn.createStatement();
			ResultSet rs;
			try {
				rs = stmt.executeQuery("SELECT SAMPLE_ID FROM SAMPLE WHERE SAMPLE_ID LIKE 'ERS%' AND EGA_ID IS NULL AND " +
						"BIOSAMPLE_AUTHORITY= 'N' AND (LAST_UPDATED BETWEEN TO_DATE ('04-NOV-13','dd-MON-yy') AND TO_DATE ('12-NOV-13','dd-MON-yy'))");
			} catch (SQLException e) {
				fail();
				e.printStackTrace();
			}
		} catch (SQLException e) {
			log.error("failed to create statement");
		}
		finally {
		
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		}
		
		
		
	}
	
	
	
}
