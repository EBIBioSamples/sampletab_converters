package uk.ac.ebi.fgpt.sampletab.pride;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PRIDEmigrateaccession {

    private final Map<String, Set<String>> groups;

    private DataSource ds = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public PRIDEmigrateaccession(Map<String, Set<String>> groups, 
            String hostname, int port, String database, String username, String password) {
        
        this.groups = groups;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            log.error("Unable to find oracle.jdbc.driver.OracleDriver", e);
            System.exit(99);
        }

        String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        OracleDataSource ds;
		try {
			ds = new OracleDataSource();
	        ds.setURL(connectURI);
	        ds.setUser(username);
	        ds.setPassword(password);
		} catch (SQLException e) {
			//bad practice, but I don't have time to go and update all the cases this is used in right now...
			throw new RuntimeException(e);
		}
        this.ds = ds;
        
    }
    
    public void process() {
        for (String group : groups.keySet()) {
            for (String sample : groups.get(group)) {
                process(group, sample);
            }
        }
        
        
        //check for same sample in multiple groups
        Map<String, Set<String>> reverse = new HashMap<String, Set<String>>();
        for (String group : groups.keySet()) {
            for (String sample : groups.get(group)) {
                if (!reverse.containsKey(sample)) {
                    reverse.put(sample, new HashSet<String>());
                }
                reverse.get(sample).add(group);
                if (reverse.get(sample).size() > 1) {
                    log.warn("Multiple groups ("+reverse.get(sample).size()+") of sample "+sample);
                }
            }
        }
    }
    
    public void process(String group, String sample) {
        //see how many accession entries there are
        //if there is only one, then it can be moved over
        //if there are more than one, 
        
        //log.trace("processing "+group+" "+sample);
        
        PreparedStatement statementA = null;
        Statement statementB = null;
        ResultSet results = null;
        Connection connect = null;
        
        try {
            connect = ds.getConnection();
            
            String sql = "SELECT * FROM SAMPLE_ASSAY WHERE " +
            		" SUBMISSION_ACCESSION LIKE 'GPR-%' " +
            		" AND USER_ACCESSION = ? " +
            		" ORDER BY ACCESSION";

            statementA = connect.prepareStatement(sql);
            statementA.setString(1, sample);
            
            results = statementA.executeQuery();
            boolean first = false;
            while (results.next()) {
                if (!first) {
                    //this is the first result
                    first = true;
                    sql = "UPDATE SAMPLE_ASSAY SET SUBMISSION_ACCESSION = 'GPR-"+group+"' WHERE ACCESSION = "+results.getInt("ACCESSION");
                    System.out.println(sql);
                    statementB = connect.createStatement();
                    statementB.execute(sql);
                    
                } else {
                    //this is a second or later result
                    sql ="UPDATE SAMPLE_ASSAY SET IS_DELETED = 1 WHERE ACCESSION = "+results.getInt("ACCESSION");
                    System.out.println(sql);
                    statementB.execute(sql);
                }
            }
            
            //do anything about groups? no not really needed
            
        } catch (SQLRecoverableException e) {
            log.warn("Problematic exception", e);
        } catch (SQLException e) {
            log.warn("Problematic exception", e);
        } finally {
            if (statementA != null){
                try {
                    statementA.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (statementB != null){
                try {
                    statementB.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (results != null){
                try {
                    results.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (connect != null){
                try {
                    connect.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
        }
    }
}
