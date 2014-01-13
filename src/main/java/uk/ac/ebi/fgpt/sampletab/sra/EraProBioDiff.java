package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.SampleTabStatus;
import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class EraProBioDiff {
	private String ftpSampleIds;
	private Logger log = LoggerFactory.getLogger(getClass());
	private FTPClient ftpClient = null;
	private Collection<String> publicEraSampleIds;
	private Collection<String> privateEraSampleIds;
	private Collection<String> publicFTPSampleIds ;
	private Collection<String> privateFTPSampleIds ;
	private final SampleTabValidator validator = new SampleTabValidator();
    
    private final SampleTabSaferParser parser = new SampleTabSaferParser(validator);
	

	public void writeEraPublicToFile(Collection<String> sampleIds){
		publicEraSampleIds = new ArrayList<String> ();
		publicEraSampleIds = sampleIds;
		
	}
	
	
	public void writeEraPrivateToFile(Collection<String> sampleIds){
		privateEraSampleIds = new ArrayList<String> ();
		privateEraSampleIds = sampleIds;
		
	}
	
	public void getSamplesFTP(){
		publicFTPSampleIds = new ArrayList<String> ();
		privateFTPSampleIds = new ArrayList<String> ();
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
			String root = ftpSampleIds+"/sra";
			ftpClient.changeWorkingDirectory(root);
			FTPFile[] sraList =  ftpClient.listDirectories();
			Collection<String> sraSubDir = new ArrayList<String> ();
			for(FTPFile srasubdir : sraList){
				sraSubDir.add(srasubdir.getName());
			}
			for(String remoteFile :sraSubDir){
			InputStream inputStream = ftpClient.retrieveFileStream(root+"/"+remoteFile+"/sampletab.txt");
			SampleData data = parser.parse(inputStream);
			Date subDate = data.msi.submissionReleaseDate;
			Date currentDate = new Date();
			if(currentDate.after(subDate)){
				Collection<SampleNode> sampleids = data.scd.getNodes(SampleNode.class);
				for (SampleNode sampleid : sampleids){
					if(!publicFTPSampleIds.contains(sampleid.getNodeName())){
					publicFTPSampleIds.add(sampleid.getNodeName());
				}
			}
		}else {
			Collection<SampleNode> sampleids = data.scd.getNodes(SampleNode.class);
			for (SampleNode sampleid : sampleids){
				if(!privateFTPSampleIds.contains(sampleid.getNodeName())){
				privateFTPSampleIds.add(sampleid.getNodeName());
			}
				}
			}
		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	private void getDiff(){
		
		
		
		
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
