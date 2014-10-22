package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
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
    private OutputStream gonePublic = null;
    private final SampleTabSaferParser parser = new SampleTabSaferParser(validator);
	

	public void writeEraPublicToFile(Collection<String> sampleIds){
		publicEraSampleIds = new ArrayList<String> ();
		publicEraSampleIds = sampleIds;
		//TODO finish
	}
	
	
	public void writeEraPrivateToFile(Collection<String> sampleIds){
		privateEraSampleIds = new ArrayList<String> ();
		privateEraSampleIds = sampleIds;
        //TODO finish
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
				Collection<SampleNode> sampleids = data.scd.getNodes(SampleNode.class); //specify sample node since it could also be a group node
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
	
	
	
	private void getPublicDiff(){
		boolean size = false ;
		int eraSize = publicEraSampleIds.size();
		int ftpSize = publicFTPSampleIds.size();
		if (eraSize == ftpSize){
			size = true;
		}
		if (publicEraSampleIds.containsAll(publicFTPSampleIds) && size){
			log.info("No updates in public sample");
		}
		if (eraSize > ftpSize){
			Collection<String> samplesPublic = new ArrayList<String> ();
			samplesPublic = publicEraSampleIds;
			//remove all the ids which are common and you will get a list of ERA samples which have been updated to go public
			samplesPublic.removeAll(publicFTPSampleIds);
			//TODO print out the collection results in a file -- need to test - might require another ftp connection from the client
			try {
			gonePublic = ftpClient.storeUniqueFileStream(ftpSampleIds+"/eraPublic.txt");
			} catch (IOException e) {
				log.debug("Error in initiating an output stream");
				e.printStackTrace();
			}
			for(String sp : samplesPublic){
			//TODO print it
			}
		}
		
		if(ftpSize > eraSize){
			Collection<String> samplesDeleted = new ArrayList<String> ();
			samplesDeleted = publicFTPSampleIds;
			//remove all the ids which are common and you will get a list of ERA samples which are no more public
			samplesDeleted.removeAll(publicEraSampleIds);
			//TODO print out the collection results in a file 
		}
		
	}
	
	private void getPrivateDiff(){
		boolean size = false ;
		int eraSize = privateEraSampleIds.size();
		int ftpSize = privateFTPSampleIds.size();
		if (eraSize == ftpSize){
			size = true;
		}
		if (privateEraSampleIds.containsAll(privateFTPSampleIds) && size){
			log.info("No updates in private sample");
		}
		if (eraSize > ftpSize){
			Collection<String> samplesPrivate = new ArrayList<String> ();
			samplesPrivate = privateEraSampleIds;
			//remove all the ids which are common and you will get a list of ERA samples which have been updated to go private
			samplesPrivate.removeAll(privateFTPSampleIds);
			//TODO print out the collection results in a file 
		}
		
		if(ftpSize > eraSize){
			Collection<String> samplesDeleted = new ArrayList<String> ();
			samplesDeleted = privateFTPSampleIds;
			//remove all the ids which are common and you will get a list of ERA samples which are no more private
			samplesDeleted.removeAll(privateEraSampleIds);
			//TODO print out the collection results in a file 
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
