package uk.ac.ebi.fgpt.sampletab.subs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;

public class TrackingManager {

    private Logger log = LoggerFactory.getLogger(getClass()); 

    private BoneCPDataSource ds = null;
    private ExperimentDAO experimentsDAO = null;
    private EventDAO eventDAO = null;
    
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
                String username = properties.getProperty("username");
                String password = properties.getProperty("password");
    
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
}
