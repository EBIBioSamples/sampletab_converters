package uk.ac.ebi.fgpt.sampletab;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import oracle.jdbc.pool.OracleDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import com.jolbox.bonecp.BoneCPDataSource;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;

public class Accessioner {
	//create prepared statements
	String stmGetAss = "SELECT ACCESSION FROM SAMPLE_ASSAY WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    String stmGetRef = "SELECT ACCESSION FROM SAMPLE_REFERENCE WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    String stmGetGrp = "SELECT ACCESSION FROM SAMPLE_GROUPS WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?";
    
    String insertAss = "INSERT INTO SAMPLE_ASSAY ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    String insertRef = "INSERT INTO SAMPLE_REFERENCE ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    String insertGrp = "INSERT INTO SAMPLE_GROUPS ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )";
    
    
    String stmGetUsr = "SELECT APIKEY, USERNAME, PUBLICEMAIL, PUBLICURL, CONTACTNAME, CONTACTEMAIL FROM USERS WHERE APIKEY LIKE ?";
    //String insertUsr = "INSERT INTO USERS (APIKEY, USERNAME, PUBLICEMAIL, PUBLICURL, CONTACTNAME, CONTACTEMAIL) VALUES (?, ?, ?, ?, ?, ?)";
    

	String stmGetUsrAss = "SELECT SUBMISSION_ACCESSION FROM SAMPLE_ASSAY WHERE ACCESSION LIKE ?";
    String stmGetUsrRef = "SELECT SUBMISSION_ACCESSION FROM SAMPLE_REFERENCE WHERE ACCESSION LIKE ?";
    String stmGetUsrGrp = "SELECT SUBMISSION_ACCESSION FROM SAMPLE_GROUPS WHERE ACCESSION LIKE ?";
    
    
    private JdbcTemplate jdbcTemplate;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public static DataSource getDataSource(String hostname, int port, String database, String dbusername, String dbpassword) throws ClassNotFoundException {
        Class.forName("oracle.jdbc.driver.OracleDriver");

        String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
        
        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl(connectURI);
        ds.setUser(dbusername);
        ds.setPassword(dbpassword);  
        
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
     * @throws DataAccessException
     */
    @Transactional
    protected synchronized String singleAccession(String name, String prefix, String username, String stmGet, String stmPut) throws DataAccessException {
        if (name == null || name.trim().length() == 0) 
            throw new IllegalArgumentException("name must be at least 1 character");
        if (prefix == null ) 
            throw new IllegalArgumentException("prefix must not be null");
        
        name = name.trim();
        username = username.toLowerCase().trim();
        
        String accession = null;
        
        try {        
	        List<String> results = jdbcTemplate.query(stmGet, new SingleStringRowMapper(), name, username);        
	        if (results.size() > 1) {
	        	throw new RuntimeException("more that one matching accession found!");
	        } else if (results.size() == 1) {
	        	accession = prefix+results.get(0);
	        } else {
	        	//if there was no put statement provided, end here
	        	if (stmPut == null) {
	        		accession = null;
	        	} else {
		        	jdbcTemplate.update(stmPut, name, username);
		        	results = jdbcTemplate.query(stmGet, new SingleStringRowMapper(), name, username);
		        	accession = prefix+results.get(0);
	        	}
	        }
        } catch (RecoverableDataAccessException e) {
        	//if it was a recoverable error, try again
        	return singleAccession(name, prefix, username, stmGet, stmPut);
        }
        return accession;
    }
    
	protected class SingleStringRowMapper implements RowMapper<String>
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
	
	public class AccessionUser {
		public final String apiKey;
		public final String username;
		public final Optional<String> publicEmail;
		public final Optional<URL> publicUrl; 
		public final Optional<String> contactName;
		public final Optional<String> contactEmail;
		
		public AccessionUser(String apiKey, String username, Optional<String> publicEmail, Optional<URL> publicUrl,
				Optional<String> contactName, Optional<String> contactEmail) {
			super();
			this.apiKey = apiKey;
			this.username = username;
			this.publicEmail = publicEmail;
			this.publicUrl = publicUrl;
			this.contactName = contactName;
			this.contactEmail = contactEmail;
		}
	}
	
	public Optional<AccessionUser> getUserForAPIkey(String apiKey) {
		List<AccessionUser> users = jdbcTemplate.query(stmGetUsr, new RowMapper<AccessionUser>(){
			@Override
			public AccessionUser mapRow(ResultSet rs, int rowNum) throws SQLException {
				//APIKEY, USERNAME, PUBLICEMAIL, PUBLICURL, CONTACTNAME, CONTACTEMAIL
				String apiKey = rs.getString("APIKEY");
				String username = rs.getString("USERNAME");
				Optional<String> publicEmail = Optional.ofNullable(rs.getString("PUBLICEMAIL"));
				Optional<URL> publicUrl;
				if (rs.getString("PUBLICURL") == null) {
					publicUrl = Optional.empty();
				} else {
					try {
						publicUrl = Optional.ofNullable(new URL(rs.getString("PUBLICURL")));
					} catch (MalformedURLException e) {
						log.error("Invalid public URL for "+username, e);
						publicUrl = Optional.empty();
					}
				}
				Optional<String> contactName = Optional.ofNullable(rs.getString("CONTACTNAME"));
				Optional<String> contactEmail = Optional.ofNullable(rs.getString("CONTACTEMAIL"));
				return new AccessionUser(apiKey, username, publicEmail, publicUrl, contactName, contactEmail);
			}}, apiKey);
		if (users.size() == 0) {
			//no user found
			return Optional.empty();
		} else if (users.size() > 1) {
			//multiple users found
			throw new IllegalStateException("Multiple users for API key "+apiKey);
		} else {
			return Optional.of(users.get(0));
		}
	}
		
	public Optional<String> getUserNameForAccession(String accession){
		//validate accession format
		String sql = null;
		if (accession.matches("SAMEA[0-9]*")) {
			sql = stmGetUsrAss;
		} else if (accession.matches("SAME[0-9]*")) {
			sql = stmGetUsrRef;
		} else if (accession.matches("SAMEG[0-9]*")) {
			sql = stmGetUsrGrp;
		} else  {
			throw new IllegalArgumentException("Invalid accession "+accession);
		}		
		
        try {        
	        List<String> results = jdbcTemplate.query(sql, new SingleStringRowMapper(), accession);        
	        if (results.size() > 1) {
	        	throw new RuntimeException("more that one matching accession found!");
	        } else if (results.size() == 1) {
	        	return Optional.of(results.get(0));
	        } else {
	        	return Optional.empty();
	        }
        } catch (RecoverableDataAccessException e) {
        	//if it was a recoverable error, try again
        	return getUserNameForAccession(accession);
		}
	}
	
	
}
