/**
 * 
 */
package uk.ac.ebi.fgpt.sampletab.subs;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

/**
 * @author drashtti
 *
 */
public class JobRegistryDriver extends AbstractDriver {

	public JobRegistryDriver(){
		
		Logger log =  LoggerFactory.getLogger(getClass()); 
	}
	
	
	public static void main(String[] args) {
		JobRegistry job = new JobRegistry();		
		try {
			job.getJobRegistry();
		} catch (SQLException e) {
		}

	}

}
