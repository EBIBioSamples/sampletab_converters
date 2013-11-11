/**
 * 
 */
package uk.ac.ebi.fgpt.sampletab.subs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

/**
 * @author drashtti
 *
 */
public class JobRegistryDriver extends AbstractDriver {
	private static Logger log =  LoggerFactory.getLogger(JobRegistryDriver.class); 

	public JobRegistryDriver(){
		
		
	}
	
	
	public static void main(String[] args)  {
		JobRegistry job = new JobRegistry();		
		try {
			job.getJobRegistry();
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

}
