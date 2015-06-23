package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.dom4j.DocumentException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.Accessioner;
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

    protected Accessioner accession;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    protected ERADAO eraDom = new ERADAO();

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
        	return;
        }
		
		List<String> submissions = eraDom.getSubmissions(minDate, maxDate);
		
		log.info("Foud "+submissions.size()+" submissions to process");
        
        if (pool == null) {
        	for (String submissionId : submissions) {
        		Callable<Void> call = new ERAUpdateCallable(outputDir, submissionId, !noconan, accession);
        		try {
					call.call();
				} catch (Exception e) {
					log.error("Problem processing "+submissionId, e);
				}
        	}
        } else {
        	Deque<Future<Void>> futures = new LinkedList<Future<Void>>();
        	for (int i = 0 ; i < submissions.size(); i++) {
        		String submissionId = submissions.get(i);
        		Callable<Void> call = new ERAUpdateCallable(outputDir, submissionId, !noconan, accession);
        		futures.push(pool.submit(call));
        		while (futures.size() > 100) {
        			log.info("No. of futures left "+futures.size());
        			Future<Void> future = futures.pollLast();
            		try {
    					future.get();
    				} catch (InterruptedException e) {
    					log.error("Problem processing "+submissions.get(i-futures.size()), e);
    				} catch (ExecutionException e) {
    					log.error("Problem processing "+submissions.get(i-futures.size()), e);
    				}
        		}
        	}
        	
    		while (futures.size() > 0) {
    			log.info("No. of futures left "+futures.size());
        		Future<Void> future = futures.pollLast();
        		try {
					future.get();
				} catch (InterruptedException e) {
					log.error("Problem processing "+submissions.get(submissions.size()-1-futures.size()), e);
				} catch (ExecutionException e) {
					log.error("Problem processing "+submissions.get(submissions.size()-1-futures.size()), e);
				}
        	}

            // run the pool and then close it afterwards
            // must synchronize on the pool object
            synchronized (pool) {
                pool.shutdown();
                try {
                    // allow 24h to execute. Rather too much, but meh
                    pool.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    log.error("Interuppted awaiting thread pool termination", e);
                }
            }
        }

    }
    
    private void setup() throws ClassNotFoundException {
    	eraDom.setup();

        pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }

        //load defaults for accessioning
        Properties oracleProperties = new Properties();
        try {
            InputStream is = getClass().getResourceAsStream("/oracle.properties");
            oracleProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource oracle.properties", e);
            return;
        }
        String hostnameAcc = oracleProperties.getProperty("hostname");
        int portAcc = new Integer(oracleProperties.getProperty("port"));
        String databaseAcc = oracleProperties.getProperty("database");
        String dbusernameAcc = oracleProperties.getProperty("username");
        String dbpasswordAcc = oracleProperties.getProperty("password");
        
        DataSource ds;
        try {
			ds = Accessioner.getDataSource(hostnameAcc, portAcc, databaseAcc, dbusernameAcc, dbpasswordAcc);
		} catch (ClassNotFoundException e) {
			log.error("Unable to create data source", e);
			return;
		}
        
        accession = new Accessioner(ds);
    }
	

	public static void main(String[] args) {
		new ERADriver().doMain(args);
	}
}
