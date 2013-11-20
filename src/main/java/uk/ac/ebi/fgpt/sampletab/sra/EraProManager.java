package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.io.InputStream;
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

}
