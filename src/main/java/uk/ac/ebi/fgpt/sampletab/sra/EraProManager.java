package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;

public class EraProManager {
	private Logger log = LoggerFactory.getLogger(getClass());
	private BoneCPDataSource ds = null;
	private String username = null;
	private String password = null;

	private static EraProManager instance = null;

	public synchronized static EraProManager getInstance() {
		if (instance == null) {
			instance = new EraProManager();
		}
		return instance;
	}

	protected synchronized BoneCPDataSource getDataSource()
			throws ClassNotFoundException {
		if (ds == null) {
			synchronized (getClass()) {
				// load defaults
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
				username = properties.getProperty("username");
				password = properties.getProperty("password");

				try {
					Class.forName("oracle.jdbc.driver.OracleDriver");
				} catch (ClassNotFoundException e) {
					log.error(
							"Unable to find oracle.jdbc.driver.OracleDriver",
							e);
					throw e;
				}

				String jdbc = "jdbc:oracle:thin//" + hostname + ":" + port
						+ "/" + database;
				log.trace("JDBC URL = " + jdbc);
				log.trace("USER = " + username);
				log.trace("PW = " + password);

				ds = new BoneCPDataSource();
				ds.setJdbcUrl(jdbc);
				ds.setUsername(username);
				ds.setPassword(password);

				ds.setPartitionCount(1);
				ds.setMaxConnectionsPerPartition(10);
				ds.setAcquireIncrement(2);
			}
		}
		return ds;
	}

	public ResultSet getSampleId(Date minDate, Date maxDate) {
		
		
		PreparedStatement stmt = null;
		Connection con = null;
		ResultSet rs = null;
		//final DateFormat fmt = new SimpleDateFormat("dd-MMM-yy");
		//Date currentDate = new Date();
		//String defaultDate = fmt.format(currentDate);
		//String startDate = fmt.format(mindate);
		if (maxDate == null){
			 maxDate = new Date(new java.util.Date().getTime());
		}
		
		//String query = "SELECT SAMPLE_ID FROM SAMPLE WHERE SAMPLE_ID LIKE 'ERS%' AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' " +
				//"AND (LAST_UPDATED BETWEEN TO_DATE ('" +startDate+ "','dd-MON-yy') AND TO_DATE ('"+endDate+"','dd-MON-yy'))";
		String query = "SELECT SAMPLE_ID FROM SAMPLE WHERE SAMPLE_ID LIKE 'ERS%' AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' " +
				"AND (LAST_UPDATED BETWEEN ? AND ?)";
		
		try {
			BoneCPDataSource ds1 = getDataSource();
			con = ds1.getConnection();
			stmt = con.prepareStatement(query);
			stmt.setDate(1, minDate);
			stmt.setDate(2, maxDate);
			rs = stmt.executeQuery(query);
			con.commit();
			if (rs == null){
				log.info("No Updates have been committed during the time period provided");
			}
		}

		catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.error("The BoneCPDatasouce class for connection to the database cannot be found :"
					+ e.getMessage());
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					rs.close();
					con.close();
				} catch (SQLException e) {
					log.error("Problem in closing the statement: "
							+ e.getMessage());

				}

			}
		}

		return rs;
	}

}
