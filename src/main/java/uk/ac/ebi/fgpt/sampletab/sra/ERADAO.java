package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class ERADAO {

    protected JdbcTemplate jdbcTemplate = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());

	public ERADAO() {
		// Auto-generated constructor stub
	}
	
	public void setup() throws ClassNotFoundException {

		Properties properties = new Properties();
		try {
			InputStream is = getClass().getResourceAsStream("/era-pro.properties");
			properties.load(is);
		} catch (IOException e) {
			log.error("Unable to read resource era-pro.properties", e);
		}
		String hostname = properties.getProperty("hostname");
		Integer port = new Integer(properties.getProperty("port"));
		String database = properties.getProperty("database");
		String username = properties.getProperty("username");
		String password = properties.getProperty("password");

		Class.forName("oracle.jdbc.driver.OracleDriver");
			
        String jdbc = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
     
        DriverManagerDataSource dataSource = new DriverManagerDataSource(jdbc, username, password);
        jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public List<String> getSubmissions(Date minDate, Date maxDate) {
        /*
select * from cv_status;
1       draft   The entry is draft.
2       private The entry is private.
3       cancelled       The entry has been cancelled.
4       public  The entry is public.
5       suppressed      The entry has been suppressed.
6       killed  The entry has been killed.
7       temporary_suppressed    the entry has been temporarily suppressed.
8       temporary_killed        the entry has been temporarily killed.
         */
		//here we get submissions about samples have either been updated, or have been made public in the date window
		//once it has been public, it can only be suppressed and killed and can't go back to public again

		//if its not an ENA sub don't do anything with it (yet)
		String query = "SELECT UNIQUE(SUBMISSION_ID) FROM SAMPLE WHERE SUBMISSION_ID LIKE 'ER%' AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' " +
				"AND STATUS_ID = 4 AND ((LAST_UPDATED BETWEEN ? AND ?) OR (FIRST_PUBLIC BETWEEN ? AND ?)) ORDER BY SUBMISSION_ID ASC";
		
		List<String> submissions = jdbcTemplate.queryForList(query, String.class, minDate, maxDate, minDate, maxDate);
		
        log.info("Got "+submissions.size()+" submission ids");
		
		return submissions;
	}

	public List<String> getPrivateSamples() {
        log.info("Getting private sample ids");
        
		String query = "SELECT UNIQUE(BIOSAMPLE_ID) FROM SAMPLE WHERE STATUS_ID > 4 AND BIOSAMPLE_ID LIKE 'SAME%' "
				+ "AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N' ORDER BY BIOSAMPLE_ID ASC";
        	
		List<String> sampleIds = jdbcTemplate.queryForList(query, String.class);
        
        log.info("Got "+sampleIds.size()+" private sample ids");
	
        return sampleIds;
	}
	
}
