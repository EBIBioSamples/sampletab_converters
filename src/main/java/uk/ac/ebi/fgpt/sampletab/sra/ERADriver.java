package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.dom4j.DocumentException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.BioSDUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;

public class ERADriver extends AbstractDriver {


    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    protected File outputDir;
    
    @Argument(required = true, index = 1, metaVar = "STARTDATE", usage = "Start date as YYYY/MM/DD")
    protected String minDateString;

    @Argument(required = true, index = 2, metaVar = "ENDDATE", usage = "End date as YYYY/MM/DD")
    protected String maxDateString;

    @Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
    protected int threads = 0;
    
    @Option(name = "--no-conan", usage = "do not trigger conan loads")
    protected boolean noconan = false;
   

    protected ExecutorService pool;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    protected JdbcTemplate jdbcTemplate;

	protected ERADriver() {
		
	}
	
    @Override
    public void doMain(String[] args){
        super.doMain(args);
        
        try {
			setup();
		} catch (ClassNotFoundException e) {
			log.error("Unable to find oracle.jdbc.driver.OracleDriver", e);
			return;
		}
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date minDate = null;
        Date maxDate = null;
        try {
            minDate = formatter.parse(minDateString);
            if (maxDateString != null) { 
                maxDate = formatter.parse(maxDateString);
            }
        } catch (ParseException e) {
        	log.error("Unable to parse date", e);
        }
        
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
				"AND STATUS_ID = 4 AND ((LAST_UPDATED BETWEEN ? AND ?) OR (FIRST_PUBLIC BETWEEN ? AND ?))";
        	
		
		List<String> submissions = jdbcTemplate.queryForList(query, String.class, minDate, maxDate, minDate, maxDate);
        
        if (pool == null) {
        	for (String submissionId : submissions) {
        		Callable<Void> call = new ERAUpdateCallable(submissionId, !noconan);
        		try {
					call.call();
				} catch (Exception e) {
					log.error("Problem processing "+submissionId, e);
				}
        	}
        } else {
        	List<Future<Void>> futures = new ArrayList<Future<Void>>();

        	for (String submissionId : submissions) {
        		Callable<Void> call = new ERAUpdateCallable(submissionId, !noconan);
        		futures.add(pool.submit(call));
        	}
        	for (int i = 0; i < futures.size(); i++) {
        		Future<Void> future = futures.get(i);
        		try {
					future.get();
				} catch (InterruptedException e) {
					log.error("Problem processing "+submissions.get(i), e);
				} catch (ExecutionException e) {
					log.error("Problem processing "+submissions.get(i), e);
				}
        	}
        }
        //TODO handle deletes
    }
    
    private void setup() throws ClassNotFoundException {

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

        pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }
        
        
    }
	
	
	public Collection<String> getPrivateSamples() {
        log.info("Getting private sample ids");
        
		String query = "SELECT UNIQUE(SAMPLE_ID) FROM SAMPLE WHERE STATUS_ID > 4 AND EGA_ID IS NULL AND BIOSAMPLE_AUTHORITY= 'N'";
        	
		List<String> sampleIds = jdbcTemplate.queryForList(query, String.class);
        
        log.info("Got "+sampleIds.size()+" private sample ids");
	
        return sampleIds;
	}
    
    protected Collection<String> getDeletions() {
        log.info("Checking deletions");
        
        Collection<String> privateSamples = getPrivateSamples();
        
        //for each private sample, check against the biosamples API and see if it is accessible
        Set<String> toDelete = new HashSet<String>();

        Collection<Future<String>> futures = new ArrayList<Future<String>>();
        if (pool == null) {
            for (String sampleID : privateSamples){
                Callable<String> c = new TestPublicCallable(sampleID);
                try {
                    if (c.call() != null) {
                        toDelete.add(sampleID);
                    }
                } catch (Exception e) {
                    log.error("Problem testing public status of "+sampleID, e);
                }
            }            
        } else {
            for (String sampleID : privateSamples){
                Callable<String> c = new TestPublicCallable(sampleID);
                Future<String> f = pool.submit(c);
                futures.add(f);
            }
            for (Future<String> f : futures) {
                String sampleID;
                try {
                    sampleID = f.get();
                    if (sampleID != null) {
                        log.info("Marking for deletion "+sampleID);
                        toDelete.add(sampleID);
                    }
                } catch (InterruptedException e) {
                    log.error("Problem testing public status", e);
                } catch (ExecutionException e) {
                    log.error("Problem testing public status", e);
                }
            }
        }
        
        for (String sampleID : privateSamples){
            log.trace("checking "+sampleID);
            if (BioSDUtils.isBioSDAccessionPublic(sampleID)) {
                log.info("adding "+sampleID);
                toDelete.add(sampleID);
            }
        }
        log.info("Finished checking deletions, found "+toDelete.size()+" samples");
        
        //now we have a list of samples to delete.
        //need to convert this into a list of BSD submissions to delete
        
        //TODO FINISH
        
        return toDelete;
    }
    

	public static void main(String[] args) {
		new ERADriver().doMain(args);
	}

    
    private class TestPublicCallable implements Callable<String> {

        private String sampleID;
        
        public TestPublicCallable(String sampleID) {
            this.sampleID = sampleID;
        }
        
        @Override
        public String call() throws Exception {
            if (BioSDUtils.isBioSDAccessionPublic(sampleID)) {
                return sampleID;
            } else {
                return null;
            }
        }
        
    }
}
