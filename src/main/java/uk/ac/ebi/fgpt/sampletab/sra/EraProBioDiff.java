package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.SampleTabStatus;
import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class EraProBioDiff {
	private String ftpSampleIds;
	private Logger log = LoggerFactory.getLogger(getClass());
	private FTPClient ftpClient = null;
	private Collection<String> publicEraSampleIds;
	private Collection<String> privateEraSampleIds;
	

	public void writeEraPublicToFile(Collection<String> sampleIds){
		publicEraSampleIds = new ArrayList<String> ();
		publicEraSampleIds = sampleIds;
		
	}
	
	
	public void writeEraPrivateToFile(Collection<String> sampleIds){
		privateEraSampleIds = new ArrayList<String> ();
		privateEraSampleIds = sampleIds;
		
	}
	
	public void getSamplesFTP(){
		Properties properties = new Properties();
		if(getFTPConnection()){
        try {
            properties.load(SampleTabStatus.class.getResourceAsStream("/sampletabconverters.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource sampletabconverters.properties", e);
        }
        ftpSampleIds = properties.getProperty("biosamples.sampletab.path");
		}
		try {
			ftpClient.changeWorkingDirectory(ftpSampleIds+"/sra");
			FTPFile[] sraList =  ftpClient.listDirectories();
			Collection<String> sraSubDir = new ArrayList<String> ();
			for(FTPFile srasubdir : sraList){
				sraSubDir.add(srasubdir.getName());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
	public boolean getFTPConnection(){
		if (ftpClient != null && !ftpClient.isConnected()){
	        closeFTPConnection();
	        ftpClient = null;
	    }
	    if (ftpClient == null){
            try {
                ftpClient = FTPUtils.connect("ftp.ebi.ac.uk");
            } catch (IOException e) {
                log.error("Unable to connect to FTP", e);
                return false;
            }
	    }
	    if (ftpClient.isConnected()) {
	        return true;
	    } else {
	        return false;
	    }
	}
	
	
	private void closeFTPConnection() {
		try {
			ftpClient.logout();
            ftpClient = null;
		} catch (IOException e) {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
					// do nothing
				}
			}
			ftpClient = null;
		}

	}
	
	
}
