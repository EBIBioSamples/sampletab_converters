package uk.ac.ebi.fgpt.sampletab;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

import com.jolbox.bonecp.BoneCPDataSource;

public class Accessioner {

    private final String hostname;
    private final int port;
    private final String database;
    private final String dbusername;
    private final String dbpassword;
    
    private final SampleTabSaferParser parser = new SampleTabSaferParser(new SampleTabValidator());
    
    private BoneCPDataSource ds = null;
    
    private Connection con = null;
    
    protected PreparedStatement stmGetAss = null;
    protected PreparedStatement stmGetRef = null;
    protected PreparedStatement stmGetGrp = null;
    protected PreparedStatement insertAss = null;
    protected PreparedStatement insertRef = null;
    protected PreparedStatement insertGrp = null;

    private Logger log = LoggerFactory.getLogger(getClass());

    public Accessioner(String host, int port, String database, String dbusername, String dbpassword) {
        // Setup the connection with the DB
        this.dbusername = dbusername;
        this.dbpassword = dbpassword;
        this.hostname = host;
        this.port = port;
        this.database = database;
    }
    
    public void close() {
        if (stmGetAss != null) {
            try {
                stmGetAss.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetAss = null;
        }
        if (stmGetRef != null) {
            try {
                stmGetRef.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetRef = null;
        }
        if (stmGetGrp != null) {
            try {
                stmGetGrp.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetGrp = null;
        }
        if (insertAss != null) {
            try {
                insertAss.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertAss = null;
        }
        if (insertRef != null) {
            try {
                insertRef.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertRef = null;
        }
        if (insertGrp != null) {
            try {
                insertGrp.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertGrp = null;
        }
        
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                //do nothing
            }
            con = null;
        }
        
        if (ds != null) {
            ds.close();
            ds = null;
        }
    }
    
    public void setup() throws SQLException, ClassNotFoundException {

        if (ds == null) {

            Class.forName("oracle.jdbc.driver.OracleDriver");

            String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
            
            ds = new BoneCPDataSource();
            ds.setJdbcUrl(connectURI);
            ds.setUsername(dbusername);
            ds.setPassword(dbpassword);
            
            //remember, there is a limit of 500 on the database
            //e.g set each accessioner to a limit of 10, and always run less than 50 cluster jobs
            ds.setPartitionCount(1); 
            ds.setMaxConnectionsPerPartition(3); 
            ds.setAcquireIncrement(1); 
        }
        
        //get a connection
        if (con == null) {
            con = ds.getConnection();
           
        } /* This has problems with which version of JDBC has the isValid method 
        
        else if (!con.isValid(5)) {
            //connection is not valid, recreate it
            try {
                con.close();
            } catch (SQLException e) {
                //do nothing
            }

            con = ds.getConnection();
        }  */
        
        //create prepared statements
        if (stmGetAss == null) stmGetAss = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_ASSAY WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        if (stmGetRef == null) stmGetRef = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_REFERENCE WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        if (stmGetGrp == null) stmGetGrp = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_GROUPS WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        
        if (insertAss == null) insertAss = con.prepareStatement("INSERT INTO SAMPLE_ASSAY ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
        if (insertRef == null) insertRef = con.prepareStatement("INSERT INTO SAMPLE_REFERENCE ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
        if (insertGrp == null) insertGrp = con.prepareStatement("INSERT INTO SAMPLE_GROUPS ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
    }
    
    public synchronized String singleAssaySample(String username) throws SQLException, ClassNotFoundException {
        //use java UUID to get a temporary sample name
        UUID uuid = UUID.randomUUID();
        String accession = singleAssaySample(uuid.toString(), username);
        return accession;
    }
    
    public synchronized String singleAssaySample(String name, String username) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();
        return singleAccession(name, "SAMEA", username, stmGetAss, insertAss);
    }
    
    public synchronized String singleReferenceSample(String name, String username) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();
        return singleAccession(name, username, "SAME", stmGetRef, insertRef);
    }
    
    public synchronized String singleGroup(String name, String username) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();     
        return singleAccession(name, "SAMEG", username, stmGetGrp, insertGrp);
    }
    
    /**
     * Internal method to bring the different types of accessioning together into one place
     * 
     * @param name
     * @param submissionID
     * @param prefix
     * @param stmGet
     * @param stmPut
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    protected synchronized String singleAccession(String name, String prefix, String username, PreparedStatement stmGet, PreparedStatement stmPut) throws SQLException, ClassNotFoundException {
        if (name == null || name.trim().length() == 0) 
            throw new IllegalArgumentException("name must be at least 1 character");
        if (prefix == null ) 
            throw new IllegalArgumentException("prefix must not be null");
        
        name = name.trim();
        
        String accession = null;
        stmGet.setString(1, name);
        stmGet.setString(2, username);
        
        log.trace(stmGet.toString());
        
        ResultSet results = stmGet.executeQuery();
        if (results.next()) {
            accession = prefix + results.getInt(1);
            results.close();
        } else {
            log.info("Assigning new accession for "+username+" : "+name);

            //insert it if not exists
            stmPut.setString(1, name);
            stmPut.setString(2, username);
            log.trace(stmPut.toString());
            stmPut.executeUpdate();

            //retrieve it
            log.trace(stmGet.toString());
            results = stmGet.executeQuery();
            results.next();
            accession = prefix + results.getInt(1);
            results.close();
        }
        
        return accession;
    }
    
}
