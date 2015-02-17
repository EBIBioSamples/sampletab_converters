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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;

/**
 * This class simplifies the interactions with the ERA-PRO database
 * 
 * @author faulcon
 *
 */
public class EraProManager {
	private Logger log = LoggerFactory.getLogger(getClass());
	private BoneCPDataSource ds = null;
	private String username = null;
	private String password = null;

	private static EraProManager instance = null;

	/**
	 * Private constructor, use getInstance() method to ensure singleton pattern is followed
	 */
	private EraProManager() {
	    
	}
	
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
					InputStream is = getClass().getResourceAsStream("/era-pro.properties");
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
					log.error("Unable to find oracle.jdbc.driver.OracleDriver", e);
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

    /**
     * Return a collection of sample ids that are public, owned by SRA, and have been updated within the date window.
     */
	public Collection<String> getUpdatesSampleId(Date minDate, Date maxDate) {
		PreparedStatement statement = null;
		Connection connection = null;
		ResultSet resultset = null;
		if (maxDate == null){
			 maxDate = new Date();
		}
		
		log.info("Getting updated sample ids");
        /*
select * from cv_status;
1       draft   The entry is draft.
2       private The entry is private.
3       cancelled       The entry has been cancelled.
4       public  The entry is public.
5       suppressed      The entry has been suppressed.
6       killed  The entry has been killed.
7       temporary_suppressed    the entry has been temporarily suppressed.
8       temporary_killed        the entry has been temporarily killed.
         */
		//here we get ones that have either been updated, or have been made public in the date window
		//once it has been public, it can only be suppressed and killed and can't go back to public again
		String query = "SELECT UNIQUE(SAMPLE_ID) FROM SAMPLE WHERE EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' " +
				"AND STATUS_ID = 4 AND ((LAST_UPDATED BETWEEN ? AND ?) OR (FIRST_PUBLIC BETWEEN ? AND ?))";
		
		Collection<String> sampleIds = new ArrayList<String>();
		
		try {
			BoneCPDataSource datasource = getDataSource();
			connection = datasource.getConnection();
			statement = connection.prepareStatement(query);
			statement.setDate(1, new java.sql.Date(minDate.getTime()));
			statement.setDate(2, new java.sql.Date(maxDate.getTime()));
            statement.setDate(3, new java.sql.Date(minDate.getTime()));
            statement.setDate(4, new java.sql.Date(maxDate.getTime()));
			resultset = statement.executeQuery();
			if (resultset == null){
				log.info("No Updates during the time period provided");
			} else {
			    while (resultset.next()) {
			        String sampleId = resultset.getString(1); //result sets are one-indexed, not zero-indexed
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
            if (resultset != null) {
                try {
                    resultset.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
		}

        log.info("Got "+sampleIds.size()+" updated sample ids");
        
		return sampleIds;
	}
	
	
	public Collection<String> getPublicSamples(){

        log.info("Getting public sample ids");
        
		PreparedStatement statment = null;
		Connection connection = null;
		ResultSet resultset = null;
		/*
select * from cv_status;
1       draft   The entry is draft.
2       private The entry is private.
3       cancelled       The entry has been cancelled.
4       public  The entry is public.
5       suppressed      The entry has been suppressed.
6       killed  The entry has been killed.
7       temporary_suppressed    the entry has been temporarily suppressed.
8       temporary_killed        the entry has been temporarily killed.
		 */
		String query = " SELECT UNIQUE(SAMPLE_ID) FROM SAMPLE WHERE STATUS_ID = 4 AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' ";
		Collection<String> sampleIds = new HashSet<String>();
		
		try {
			BoneCPDataSource datasource = getDataSource();
			connection = datasource.getConnection();
			statment = connection.prepareStatement(query);
			resultset = statment.executeQuery();
			if (resultset == null){
				log.info("No Public samples found!");
			} else {
			    while (resultset.next()) {
			        String sampleId = resultset.getString(1); //result sets are one-indexed, not zero-indexed
                    //assume all sample ids are unique because its a set collection and the SQL has a unique function
                    sampleIds.add(sampleId);
                    log.trace("adding sample id "+sampleId);
			    }
			}
			
		} catch (SQLException e) {
		    log.error("Problem acessing database", e);
		} catch (ClassNotFoundException e) {
			log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
		} finally {
		    //close each of these separately in case of errors
            if (resultset != null) {
                try {
                    resultset.close();
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
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
		}

        log.info("Got "+sampleIds.size()+" public sample ids");
		
		return sampleIds;
		
	}
	
	
	
	public Collection<String> getPrivateSamples() {
        log.info("Getting private sample ids");
        
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultset = null;
        //only need to deal with those that were once public
        String query = "SELECT UNIQUE(SAMPLE_ID) FROM SAMPLE WHERE STATUS_ID > 4 AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N'";
        Collection<String> sampleIds = new HashSet<String>();
        try {
            BoneCPDataSource datasource = getDataSource();
            connection = datasource.getConnection();
            statement = connection.prepareStatement(query);
            resultset = statement.executeQuery();
            if (resultset == null) {
                log.info("No Private samples found!");
            } else {
                while (resultset.next()) {
                    String sampleId = resultset.getString(1); // result sets are one-indexed, not zero-indexed
                    //assume all sample ids are unique because its a set collection and the SQL has a unique function
                    sampleIds.add(sampleId);
                }
            }
        } catch (SQLException e) {
            log.error("Problem acessing database", e);
        } catch (ClassNotFoundException e) {
            log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
        } finally {
            // close each of these separately in case of errors
            if (resultset != null) {
                try {
                    resultset.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
        }
        
        log.info("Got "+sampleIds.size()+" private sample ids");
	
        return sampleIds;
	}
	

	/**
	 * Retuns a mapping between ENA sample ID and BioSample ID
	 * Only looks for sample updated or released within a specific date window 
	 * 
	 * 
	 * @param minDate
	 * @param maxDate
	 * @return
	 */
    public Map<String, String> getSRABioSampleMappingForDateRange(Date minDate, Date maxDate) {
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultset = null;
        log.info("Getting all public sample - biosample ids");
        String query = "SELECT SAMPLE_ID, BIOSAMPLE_ID FROM SAMPLE WHERE STATUS_ID = 4 AND EGA_ID IS NULL "
                + "AND ((LAST_UPDATED BETWEEN ? AND ?) OR (FIRST_PUBLIC BETWEEN ? AND ?))";

        Map<String, String> sampleIds = new HashMap<String, String>();

        try {
            BoneCPDataSource datasource = getDataSource();
            connection = datasource.getConnection();
            statement = connection.prepareStatement(query);
            statement.setDate(1, new java.sql.Date(minDate.getTime()));
            statement.setDate(2, new java.sql.Date(maxDate.getTime()));
            statement.setDate(3, new java.sql.Date(minDate.getTime()));
            statement.setDate(4, new java.sql.Date(maxDate.getTime()));
            resultset = statement.executeQuery();
            if (resultset == null) {
                log.info("No Updates for sample mapping during the time period provided");
            } else {
                while (resultset.next()) {
                    String sampleId = resultset.getString(1); // result sets are one-indexed,
                    String biosampleId = resultset.getString(2);
                    if (sampleId != null && biosampleId != null) {
                        sampleIds.put(sampleId, biosampleId);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Problem acessing database", e);
        } catch (ClassNotFoundException e) {
            log.error("The BoneCPDatasouce class for connection to the database cannot be found", e);
        } finally {
            // close each of these separately in case of errors
            if (resultset != null) {
                try {
                    resultset.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
        }

        log.info("Got " + sampleIds.size() + " public sample ids");

        return sampleIds;

    }

}
