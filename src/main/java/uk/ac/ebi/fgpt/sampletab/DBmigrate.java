package uk.ac.ebi.fgpt.sampletab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBmigrate {

    private Logger log = LoggerFactory.getLogger(getClass());
    
    
    
    private Connection getProdOracleConnection() throws ClassNotFoundException, SQLException{
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw e;
        }
        
        log.info("Found oracle.jdbc.driver.OracleDriver");

        String hostname = "burns.ebi.ac.uk";
        String port = "1521";
        String database = "DWEP";
        String username = "bsd_acc";
        String password = "bsd_acc";
        
        String url = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Unable to connect to "+url, e);
            throw e;
        }

        log.info("connected to "+url+" with username/password "+username+"/"+password);
        return con;
    }
    
    private Connection getTestOracleConnection() throws ClassNotFoundException, SQLException{
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw e;
        }
        
        log.info("Found oracle.jdbc.driver.OracleDriver");

        String hostname = "bart.ebi.ac.uk";
        String port = "1521";
        String database = "BIOSDTST";
        String username = "bsd_acc";
        String password = "bsd_acc";
        
        String url = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Unable to connect to "+url, e);
            throw e;
        }

        log.info("connected to "+url+" with username/password "+username+"/"+password);
        return con;
    }
    
    
    private Connection getMySQLConnection() throws ClassNotFoundException, SQLException{
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw e;
        }
        log.info("Found com.mysql.jdbc.Driver");
        
        //production environment
        String hostname = "mysql-ae-autosubs.ebi.ac.uk";
        String port = "4091";
        String database = "ae_autosubs";
        String username = "curator";
        String password = "troajsp";
        
        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database;

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Unable to connect to "+url, e);
            throw e;
        }

        log.info("connected to "+url+" with username/password "+username+"/"+password);
        
        return con;
        
    }
    
    public void copyTable(String table_name, Connection target, Connection source){

        Statement statement = null;
        try {
            statement = source.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM "+table_name);
            while (rs.next()) {
                Integer accession = rs.getInt("accession");
                String userAccession = rs.getString("user_accession");
                userAccession = userAccession.replace("'", "''");
                String submissionAccession = rs.getString("submission_accession");
                submissionAccession = submissionAccession.replace("'", "''");
                Date assigned = rs.getDate("date_assigned");
                Integer deleted = rs.getInt("is_deleted");
                
                String sql = "INSERT INTO "+table_name+" VALUES ( '"+accession+"' , '"+userAccession+"' , '"+submissionAccession+"' , to_date('"+assigned+"', 'YYYY-MM-DD') , '"+deleted+"' )";

                Statement substatement = null;
                try {
                    substatement = target.createStatement();
                    substatement.execute(sql);
                    substatement.close();
                } catch (SQLException e) {
                    log.info(sql);
                    log.warn ("Unable to process "+accession, e);
                } finally {
                    if (substatement != null){
                        try {
                            substatement.close();
                        } catch (SQLException e) {
                            //do nothing
                        }
                    }
                }
                
            }
        } catch (SQLException e) {
            log.error("Unable to process SQL", e);
            return;
        } finally {
            if (statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
        }
    }

    public void doMain() {
        Connection source = null;
        Connection target = null;   
        
        try {
            //source = getMySQLConnection();
            source = getProdOracleConnection();
        } catch (ClassNotFoundException e) {
            log.error("Unable to find driver", e);
            return;
        } catch (SQLException e) {
            log.error("Unable to connect to execute", e);
            return;
        }  
        
        try {
            target = getTestOracleConnection();
        } catch (ClassNotFoundException e) {
            log.error("Unable to find driver", e);
            return;
        } catch (SQLException e) {
            log.error("Unable to connect to execute", e);
            return;
        }
   
        
        copyTable("sample_reference", target, source);
        copyTable("sample_assay", target, source);
        copyTable("sample_groups", target, source);
        
    }

    public static void main(String[] args) {
        new DBmigrate().doMain();
    }
    
}
