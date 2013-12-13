package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
					log.error("Unable to read resource era-pro.properties", e);
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
                String jdbc = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
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
    public Collection<String> getSampleId(Date minDate) {
        return getUpdatesSampleId(minDate, new Date());
    }

	public Collection<String> getUpdatesSampleId(Date minDate, Date maxDate) {
		PreparedStatement stmt = null;
		Connection con = null;
		ResultSet rs = null;
		if (maxDate == null){
			 maxDate = new Date();
		}
		
		String query = "SELECT SAMPLE_ID FROM SAMPLE WHERE EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' " +
				"AND STATUS_ID = 4 AND (LAST_UPDATED BETWEEN ? AND ?)";
		
		Collection<String> sampleIds = new ArrayList<String>();
		
		try {
			BoneCPDataSource ds1 = getDataSource();
			con = ds1.getConnection();
			stmt = con.prepareStatement(query);
			stmt.setDate(1, new java.sql.Date(minDate.getTime()));
			stmt.setDate(2, new java.sql.Date(maxDate.getTime()));
			rs = stmt.executeQuery();
			if (rs == null){
				log.info("No Updates during the time period provided");
			} else {
			    while (rs.next()) {
			        String sampleId = rs.getString(1); //result sets are one-indexed, not zero-indexed
			        if (!sampleIds.contains(sampleId)) {
			            sampleIds.add(sampleId);
			        }
			    }
			}
		} catch (SQLException e) {
		    log.error("Problem acessing database", e);
		} catch (ClassNotFoundException e) {
			log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
		} finally {
		    //close each of these separately in case of errors
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
		}

		return sampleIds;
	}
	
	
	public Collection<String> getPublicSamples(){
		
		PreparedStatement statment = null;
		Connection con = null;
		ResultSet rs = null;
		String query = " SELECT SAMPLE_ID FROM SAMPLE WHERE STATUS_ID ='4'";
		Collection<String> sampleIds = new ArrayList<String>();
		
		try{
			BoneCPDataSource ds1 = getDataSource();
			con = ds1.getConnection();
			statment = con.prepareStatement(query);
			rs = statment.executeQuery();
			if (rs == null){
				log.info("No Public samples found!");
			} else {
			    while (rs.next()) {
			        String sampleId = rs.getString(1); //result sets are one-indexed, not zero-indexed
			        if (!sampleIds.contains(sampleId)) {
			            sampleIds.add(sampleId);
			        }
			    }
			}
			
		} catch (SQLException e) {
		    log.error("Problem acessing database", e);
		} catch (ClassNotFoundException e) {
			log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
		} finally {
		    //close each of these separately in case of errors
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (statment != null) {
                try {
                    statment.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
		}
		
		
		return sampleIds;
		
	}
	
	
	
	public Collection<String> getPrivateSamples(){
		PreparedStatement stmt = null;
		Connection con = null;
		ResultSet rs = null;
		String query = "SELECT SAMPLE_ID FROM SAMPLE WHERE STATUS_ID !='4'";
		Collection<String> sampleIds = new ArrayList<String> ();
		try{
			BoneCPDataSource ds1 = getDataSource();
		con = ds1.getConnection();
		stmt = con.prepareStatement(query);
		rs = stmt.executeQuery();
		if (rs == null){
			log.info("No Private samples found!");
		} else {
		    while (rs.next()) {
		        String sampleId = rs.getString(1); //result sets are one-indexed, not zero-indexed
		        if (!sampleIds.contains(sampleId)) {
		            sampleIds.add(sampleId);
		        	}	
		    	}
			}
		} catch (SQLException e) {
	    log.error("Problem acessing database", e);
		} catch (ClassNotFoundException e) {
		log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
		} finally {
	    //close each of these separately in case of errors
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
	}
	
	return sampleIds;
	}

}
