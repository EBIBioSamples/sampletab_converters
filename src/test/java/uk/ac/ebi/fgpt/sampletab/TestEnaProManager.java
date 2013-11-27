package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
		
		//String url = "jdbc:oracle:thin:@ora-vm-009.ebi.ac.uk:1541:ERAPRO" ;
	
		//String username = "era_reader";
		
		//String password = "reader";
		
		//String driver = "oracle.jdbc.driver.OracleDriver";
		
		Connection conn = null;
		 
		
		try {
		conn = DriverManager.getConnection(jdbc, username, password);
		
		}catch(Exception e){
		
		fail();
		
		} finally {
		
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		}
		
	}
	
	
	
}
