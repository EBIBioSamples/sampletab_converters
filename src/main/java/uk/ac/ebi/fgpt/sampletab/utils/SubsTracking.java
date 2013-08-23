package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AccessionerDriver;

import com.jolbox.bonecp.BoneCPDataSource;

public class SubsTracking {

    private Logger log = LoggerFactory.getLogger(getClass()); 

    private BoneCPDataSource ds = null;
    
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
    private static SubsTracking instance = null;
    
    public synchronized static SubsTracking getInstance() {
        if (instance == null) {
            instance = new SubsTracking();
        }
        return instance;
    }
    
    protected synchronized Connection getConnection() throws SQLException, ClassNotFoundException {
        if (ds == null) {
            String hostname;
            Integer port;
            String database;
            String username;
            String password;

            //load defaults
            Properties properties = new Properties();
            try {
                InputStream is = AccessionerDriver.class.getResourceAsStream("/substracking.properties");
                properties.load(is);
            } catch (IOException e) {
                log.error("Unable to read resource oracle.properties", e);
            }
            hostname = properties.getProperty("hostname");
            port = new Integer(properties.getProperty("port"));
            database = properties.getProperty("database");
            username = properties.getProperty("username");
            password = properties.getProperty("password");
                        
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                log.error("Unable to find com.mysql.jdbc.Driver", e);
                throw e;
            }
            ds = new BoneCPDataSource();
            ds.setJdbcUrl("jdbc:mysql://"+hostname+":"+port+"/"+database);
            ds.setUsername(username);
            ds.setPassword(password);
        }
        return ds.getConnection();
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
    
    public void registerEventStart(String experimentID, String eventType, Date start, File logFile) {
        Connection c = null;
        try {
            c = getConnection();
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        } catch (ClassNotFoundException e) {
            log.error("Unable to register event", e);
            return;
        }
        
        Statement st;
        try {
            st = c.createStatement();
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        }
        
        //NB not cross-platform!
        String machineName = null;
        try {
            machineName = getHostname();
        } catch (IOException e) {
            log.error("Unable to register event", e);
            return;
        }
        
        String startString = df.format(start);
        String user = System.getProperty("user.name");
        
        try {
            String sql = "INSERT INTO events (" 
            		+"experiment_id, event_type, " 
            		+"start_time, machine, operator, " 
            		+"log_file, is_deleted)" 
            		+" VALUES " 
            		+"('"+experimentID+"', '"+eventType+"', "
            		+"'"+startString+"', '"+machineName+"', '"+user+"',"
            		+"'"+logFile+"', 0)";
            log.trace(sql);
            st.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        }
    }
    
    public void registerEventEnd(String experimentID, String eventType, Date start, Date end, boolean successful) {
        Connection c = null;
        try {
            c = getConnection();
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        } catch (ClassNotFoundException e) {
            log.error("Unable to register event", e);
            return;
        }
        
        Statement st;
        try {
            st = c.createStatement();
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        }

        String startString = df.format(start);
        String endString = df.format(end);
        int successValue = 0;
        if (successful) successValue = 1;
        
        try {
            String sql = "UPDATE events SET "
                    +"end_time='"+endString+"', was_successful="+successValue+" "
                    +"WHERE experiment_id='"+experimentID+"' AND event_type='"+eventType+"' AND start_time='"+startString+"' ";
            log.trace(sql);
            st.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("Unable to register event", e);
            return;
        }
    }
}
