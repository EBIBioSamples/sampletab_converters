package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IMSRTabWebSummary {
	// logging
	private Logger log = LoggerFactory.getLogger(getClass());
	
	//Singleton instance
	private static IMSRTabWebSummary instance = null;

	private final String url = "http://www.findmice.org/fetch?page=imsrReport&report=siteCounts&print=data";
			
	public final List<String> sites = new ArrayList<String>();
	public final List<String> facilities = new ArrayList<String>();
	public final List<Integer> strainss = new ArrayList<Integer>();
	public final List<Integer> esliness = new ArrayList<Integer>();
	public final List<Integer> totals = new ArrayList<Integer>();
	public final List<Date> updates = new ArrayList<Date>();
	
	private IMSRTabWebSummary(){
		// private constructor
		BufferedReader input;
		String line;
		int lineid = 0;
		SimpleDateFormat simpledateformat = new SimpleDateFormat("MMM dd, yyyy");
		log.info("Retrieving IMSR summary information");
		try {
			//setup the input as buffered characters
			input = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
			//no go through each line in turn
			while ((line = input.readLine()) != null){
				if (lineid <= 2){
					//read headers
					lineid ++;
					continue;
				} else {
					//read data
					String[] entries = line.split("\t");
					sites.add(entries[0]);
					facilities.add(entries[1]);
					strainss.add(Integer.parseInt(entries[2]));
					esliness.add(Integer.parseInt(entries[3]));
					totals.add(Integer.parseInt(entries[4]));
					updates.add(simpledateformat.parse(entries[5]));
					lineid ++;
				}
			}
			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
	}
	 
    public static IMSRTabWebSummary getInstance() {
    	if (instance == null){
    		instance = new IMSRTabWebSummary();
    	}
        return instance;
    }
	
}
