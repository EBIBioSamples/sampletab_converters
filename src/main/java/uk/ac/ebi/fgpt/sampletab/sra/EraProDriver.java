package uk.ac.ebi.fgpt.sampletab.sra;

import java.sql.Date;
import java.sql.ResultSet;

import org.kohsuke.args4j.Argument;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;


public class EraProDriver extends AbstractDriver {
	
	
	@Argument(required=true, index=0, metaVar="STARTDATE", usage = "Start Date")
    	protected Date minDate;
	 

	@Argument(required=false, index=1, metaVar="ENDDATE", usage = "End Date")
    	protected Date maxDate;

	 public static void main(String[] args) {
	        new EraProDriver().doMain(args);
	    } 
	 
	 
	 @Override
	 public void doMain(String[] args){
		    super.doMain(args);
		    
		   EraProManager era = new EraProManager();
		   ResultSet rs = era.getSampleId(minDate,maxDate);
		   
	 }
	 
	 
	 
}
