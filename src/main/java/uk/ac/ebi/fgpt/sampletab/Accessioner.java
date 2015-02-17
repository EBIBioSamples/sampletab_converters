package uk.ac.ebi.fgpt.sampletab;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import com.jolbox.bonecp.BoneCPDataSource;

public class Accessioner {
	//create prepared statements
	String stmGetAss = "SELECT ACCESSION FROM SAMPLE_ASSAY WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    String stmGetRef = "SELECT ACCESSION FROM SAMPLE_REFERENCE WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    String stmGetGrp = "SELECT ACCESSION FROM SAMPLE_GROUPS WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    
    String insertAss = "INSERT INTO SAMPLE_ASSAY ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    String insertRef = "INSERT INTO SAMPLE_REFERENCE ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    String insertGrp = "INSERT INTO SAMPLE_GROUPS ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    
    private JdbcTemplate jdbcTemplate;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public static DataSource getDataSource(String hostname, int port, String database, String dbusername, String dbpassword) throws ClassNotFoundException {
        // Setup the connection with the DB
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw e;
        }

        Class.forName("oracle.jdbc.driver.OracleDriver");

        String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
        
        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl(connectURI);
        ds.setUsername(dbusername);
        ds.setPassword(dbpassword);
        
        //remember, there is a limit of 500 on the database
        //e.g set each accessioner to a limit of 10, and always run less than 50 cluster jobs
        ds.setPartitionCount(1); 
        ds.setMaxConnectionsPerPartition(3); 
        ds.setAcquireIncrement(1);
    	return ds;
    }
    
    public Accessioner(DataSource dataSource) {
    	setDataSource(dataSource);
    }
    
    public void setDataSource(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    public synchronized String singleAssaySample(String username) {
        //use java UUID to get a temporary sample name
        String accession = singleAssaySample(UUID.randomUUID().toString(), username);
        //technically, this may have collisions but they should be very rare, if ever
        return accession;
    }
    
    public synchronized String singleAssaySample(String name, String username)  {
        return singleAccession(name, "SAMEA", username, stmGetAss, insertAss);
    }
    
    public synchronized String singleReferenceSample(String name, String username) {
        return singleAccession(name, "SAME", username, stmGetRef, insertRef);
    }
    
    public synchronized String singleGroup(String name, String username) {
        return singleAccession(name, "SAMEG", username, stmGetGrp, insertGrp);
    }
    
    public synchronized boolean testAssaySample(String name, String username)  {
        return testAccession(name, "SAMEA", username, stmGetAss);
    }
    
    public synchronized boolean testReferenceSample(String name, String username) {
        return testAccession(name, "SAME", username, stmGetRef);
    }
    
    public synchronized boolean testGroup(String name, String username) {
        return testAccession(name, "SAMEG", username, stmGetGrp);
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
    @Transactional
    protected synchronized String singleAccession(String name, String prefix, String username, String stmGet, String stmPut) {
        if (name == null || name.trim().length() == 0) 
            throw new IllegalArgumentException("name must be at least 1 character");
        if (prefix == null ) 
            throw new IllegalArgumentException("prefix must not be null");
        
        name = name.trim();
        List<String> results = jdbcTemplate.query(stmGetAss, new AccessionRowMapper());        
        if (results.size() > 1) {
        	throw new RuntimeException("more that one matching accession found!");
        } else if (results.size() == 1) {
        	return prefix+results.get(0);
        } else {
        	jdbcTemplate.update(stmPut, new Object[]{name, username});
        	results = jdbcTemplate.query(stmGetAss, new AccessionRowMapper());
        	return prefix+results.get(0);
        }
    }
    
    /**
     * Internal method to test if an accession has been assigned to a username and name
     * @param name
     * @param prefix
     * @param username
     * @param stmGet
     * @return
     */
    private boolean testAccession(String name, String prefix, String username, String stmGet) {
        if (name == null || name.trim().length() == 0) 
            throw new IllegalArgumentException("name must be at least 1 character");
        if (prefix == null ) 
            throw new IllegalArgumentException("prefix must not be null");
        
        name = name.trim();
        List<String> results = jdbcTemplate.query(stmGetAss, new AccessionRowMapper());
        if (results.size() > 1) {
        	throw new RuntimeException("more that one matching accession found!");
        } else if (results.size() == 1) {
        	return true;
        } else {
        	return false;
        }
    }

	protected class AccessionRowMapper implements RowMapper<String>
	{
		public String mapRow(ResultSet rs, int rowNum) throws SQLException {
			return rs.getString(1);
		}
	}
    
}
