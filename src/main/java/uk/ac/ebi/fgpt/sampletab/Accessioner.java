package uk.ac.ebi.fgpt.sampletab;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;

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
        ds.setMinConnectionsPerPartition(1);
        ds.setMaxConnectionsPerPartition(10); 
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
    
    
    public synchronized String retrieveAssaySample(String name, String username)  {
        return singleAccession(name, "SAMEA", username, stmGetAss, null);
    }
    
    public synchronized String retrieveReferenceSample(String name, String username) {
        return singleAccession(name, "SAME", username, stmGetRef, null);
    }
    
    public synchronized String retrieveGroup(String name, String username) {
        return singleAccession(name, "SAMEG", username, stmGetGrp, null);
    }
    
    
    public synchronized boolean testAssaySample(String name, String username)  {
    	return singleAccession(name, "SAMEA", username, stmGetAss, null) != null;
    }
    
    public synchronized boolean testReferenceSample(String name, String username) {
    	return singleAccession(name, "SAME", username, stmGetRef, null) != null;
    }
    
    public synchronized boolean testGroup(String name, String username) {
    	return singleAccession(name, "SAMEG", username, stmGetGrp, null) != null;
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
        username = username.toLowerCase().trim();
        
        List<String> results = jdbcTemplate.query(stmGet, new AccessionRowMapper(), name, username);        
        if (results.size() > 1) {
        	throw new RuntimeException("more that one matching accession found!");
        } else if (results.size() == 1) {
        	return prefix+results.get(0);
        } else {
        	//if there was no put statement provided, end here
        	if (stmPut == null) {
        		return null;
        	} else {
	        	jdbcTemplate.update(stmPut, name, username);
	        	results = jdbcTemplate.query(stmGet, new AccessionRowMapper(), name, username);
	        	return prefix+results.get(0);
        	}
        }
    }
    
	protected class AccessionRowMapper implements RowMapper<String>
	{
		public String mapRow(ResultSet rs, int rowNum) throws SQLException {
			return rs.getString(1);
		}
	}

	public SampleData convert(SampleData sd) throws ParseException {

		// now assign and retrieve accessions for samples that do not have them
		Collection<SampleNode> samples = sd.scd.getNodes(SampleNode.class);
		for (SampleNode sample : samples) {
			if (sample.getSampleAccession() == null) {
				String accession;
				if (sd.msi.submissionReferenceLayer) {
					accession = singleReferenceSample(sample.getNodeName(),
							sd.msi.submissionIdentifier);
				} else {
					accession = singleAssaySample(sample.getNodeName(),
							sd.msi.submissionIdentifier);
				}
				sample.setSampleAccession(accession);
			}
		}

		// now assign and retrieve accessions for groups that do not have them
		Collection<GroupNode> groups = sd.scd.getNodes(GroupNode.class);
		for (GroupNode group : groups) {
			if (group.getGroupAccession() == null) {
				String accession = singleGroup(group.getNodeName(),
						sd.msi.submissionIdentifier);
				group.setGroupAccession(accession);
			}
		}
		return sd;
	}
}
