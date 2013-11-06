package uk.ac.ebi.fgpt.sampletab.subs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import uk.ac.ebi.fg.biosd.model.organizational.MSI;
import uk.ac.ebi.fg.core_model.resources.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fg.biosd.model.application_mgmt.JobRegisterEntry;
import uk.ac.ebi.fg.biosd.model.application_mgmt.JobRegisterEntry.Operation;
import uk.ac.ebi.fg.biosd.model.persistence.hibernate.application_mgmt.JobRegisterDAO;
import com.jolbox.bonecp.BoneCPDataSource;

public class TrackingManager {

    private Logger log = LoggerFactory.getLogger(getClass()); 

    private BoneCPDataSource ds = null;
    private ExperimentDAO experimentsDAO = null;
    private EventDAO eventDAO = null;
    private String username = null;
    private String password = null;
    
    //date format to write into database as a string
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
    private static TrackingManager instance = null;
    
    public synchronized static TrackingManager getInstance() {
        if (instance == null) {
            instance = new TrackingManager();
        }
        return instance;
    }
    
    protected ExperimentDAO getExperimentDAO() throws ClassNotFoundException {
        if (experimentsDAO == null) {
            experimentsDAO = new ExperimentDAO(getDataSource());
        }
        return experimentsDAO;
    }
    
    protected EventDAO getEventDAO() throws ClassNotFoundException {
        if (eventDAO == null) {
            eventDAO = new EventDAO(getDataSource());
        }
        return eventDAO;
    }
    
    protected synchronized BoneCPDataSource getDataSource() throws ClassNotFoundException {
        if (ds == null) {
            synchronized (getClass()){    
                //load defaults
                Properties properties = new Properties();
                try {
                    InputStream is = getClass().getResourceAsStream("/substracking.properties");
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
                    Class.forName("com.mysql.jdbc.Driver");
                } catch (ClassNotFoundException e) {
                    log.error("Unable to find com.mysql.jdbc.Driver", e);
                    throw e;
                }
                            
                String jdbc = "jdbc:mysql://"+hostname+":"+port+"/"+database;
                log.trace("JDBC URL = "+jdbc);
                log.trace("USER = "+username);
                log.trace("PW = "+password);
                            
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
    
    private String getHostname() throws IOException {
        Process p = Runtime.getRuntime().exec("hostname");
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = br.readLine()) != null) {
           builder.append(line);
           builder.append(System.getProperty("line.separator"));
        }
        String result = builder.toString();
        return result;
    }

    public Event registerEventStart(String submissionIdentifier, String eventType) {
        Event event = null;
        try {
            event = new Event();
        
            try {
                event.setMachine(getHostname());
            } catch (IOException e) {
                log.warn("Unable to discover hostname", e);
                return null;
            }
            
            event.setOperator(System.getProperty("user.name"));
            
            event.setStartTime(new Date());
            
            event.setEventType(eventType);
            
            Experiment experiment = null;
            try {
                experiment = getExperimentDAO().getExperiment(submissionIdentifier);
            } catch (ClassNotFoundException e) {
                log.error("Unable to find database connector", e);
                return null;
            }
            int experimentID = experiment.getId();
            event.setExperiment(experimentID);
            
            try {
                getEventDAO().storeEvent(event);
            } catch (ClassNotFoundException e) {
                log.error("Unable to find database connector", e);
                return null;
            }
                    
        } catch (Exception e) {
            log.warn("Problem registering event start", e);
            return null;
        }
        return event;
    }
    
    public void registerEventEnd(Event event) {
        if (event == null) {
            log.warn("Cannot register end of null event");
            return;
        }
        try {
            event.setEndTime(new Date());
            
            try {
                getEventDAO().storeEvent(event);
            } catch (ClassNotFoundException e) {
                log.error("Unable to find database connector", e);
            }
        } catch (Exception e) {
            log.warn("Problem registering event end", e);
        }
    }
    
    
	public void getJobRegistry() throws SQLException {
		EntityManagerFactory emf = Resources.getInstance()
				.getEntityManagerFactory();
		EntityManager em = emf.createEntityManager();

		JobRegisterDAO jrDao = new JobRegisterDAO(em);

		List<JobRegisterEntry> logs = jrDao.find(1, MSI.class);
		log.error("find with entityType didn't work!", 1, logs.size());
		for (JobRegisterEntry l : logs) {
			String accession = l.getAcc();
			Operation operation = l.getOperation();
			Date timestamp = l.getTimestamp();
			String id = getExpermentId(accession);
			writeToDatabase(id, operation.toString(), timestamp);
		}

	}

	private void writeToDatabase(String id, String operation, Date timestamp) {

		String query = "INSERT INTO events (experiment_id, event_type,start_time, end_time) VALUES ( '"
				+ id
				+ "','RelationalDatabase_"
				+ operation
				+ "','"
				+ timestamp
				+ "','" + timestamp + "')";
		Statement stmt = null;
		Connection con = null;
		try {
			BoneCPDataSource ds1 = getDataSource();
			con = ds1.getConnection();
			stmt = con.createStatement();
			int change = stmt.executeUpdate(query);
			log.info("Number of rows updated = " + change);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			log.error("The BoneCPDatasouce class for connection to the database cannot be found :"
					+ e.getMessage());

		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					con.close();
				} catch (SQLException e) {
					log.error("Problem in closing the connection: "
							+ e.getMessage());

				}
			}
		}
	
	}


	private String getExpermentId(String accession) {
		String id = null;
		Statement stmt = null;
		Connection con = null;
		ResultSet rs = null;
		String query = "SELECT id from experiments WHERE accession='"
				+ accession + "'";
		try {
			BoneCPDataSource ds1 = getDataSource();
			con = ds1.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			if (rs.isFirst() && rs.isLast()){
				id = rs.getString("id");
				}
			else{
					log.error("The experiment "
							+ accession
							+ "is not present in the experiments table in the SubsTracking database");
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

		return id;
		}
		
	}

