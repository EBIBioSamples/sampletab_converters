package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabStatusRunnable implements Callable<Void> {
	
	private static final Date now = new Date(); 
		
	private static final SampleTabSaferParser stparser = new SampleTabSaferParser();
	
	private final File sampletabFile;
	private final File ftpDir;
	
	private Boolean shouldBePublic = false;
    private Boolean isOnFTP = false;
	private Boolean isFTPUpToDate = false;
	
    private SampleData sd = null;
    
    
    private Logger log = LoggerFactory.getLogger(getClass());
    

	public SampleTabStatusRunnable(File sampletabFile, File ftpDir) {
		this.sampletabFile = sampletabFile;

		if (ftpDir == null) {
			throw new IllegalArgumentException("ftpDir cannot be null");
		}
		if (!ftpDir.exists()) {
			throw new IllegalArgumentException("ftpDir must exist ("+ftpDir+")");
		}
		this.ftpDir = ftpDir;
	}
	
	@Override
	public Void call() throws Exception {
		
    	//each input can in one the following states:
    	//  currently public and should stay public <- do nothing
    	//  currently public and should be private <- the bad state
    	//  currently private and should be public
    	//  currently private and should stay private <- do nothing
    	//  currently not up to date and should be public
    	//  currently not up to date and should be private
	    		
        synchronized(stparser) {
            try {
                sd = stparser.parse(sampletabFile);
            } catch (ParseException e) {
                log.error("Unable to parse file "+sampletabFile, e);
            	throw e;
            }
        }
    	
        //calculate shouldBePublic
        //use release date inside file
    	if (sd != null) {
    		if (sd.msi.submissionReleaseDate == null || sd.msi.submissionReleaseDate.before(now)) {
        		//should be public
    			shouldBePublic = true;
    		} else if (sd.msi.submissionReleaseDate.after(now)) {
        		//should be private
    			shouldBePublic = false;
    		}
    	}
    	            	
        //calculate isPublic
		File ftpFile = getFTPFile();
    	if (ftpFile.exists()) {
    		isOnFTP = true;
            if (sampletabFile.lastModified() > ftpFile.lastModified()) {
                isFTPUpToDate = false;
            } else {
                isFTPUpToDate = true;
            }
    	} else {
    		isOnFTP = false;
    	}
    	
    	//log.debug(sampletabFile.getName()+" "+shouldBePublic+" "+isOnFTP+" "+isFTPUpToDate);
    	
    	
    	//now we have the information, determine what we need to do
    	
    	if (shouldBePublic){
    	    if (!isOnFTP || !isFTPUpToDate) {
    	        //copy to FTP
    	        copyToFTP();
    	    }
    	} else if (!shouldBePublic) {
            if (isOnFTP) {
                //remove from FTP
                removeFromFTP();
            }
    	}
    	return null;
	}
	
    private File getFTPFile() {
        File ftpSubDir = new File(ftpDir, SampleTabUtils.getSubmissionDirPath(sd.msi.submissionIdentifier));
        File ftpFile = new File(ftpSubDir, "sampletab.txt");
        return ftpFile;
        
    }
    
    private void removeFromFTP() {
        log.trace("Removing from FTP "+sd.msi.submissionIdentifier);
        File ftpFile = getFTPFile();
        if (ftpFile.exists()) {
            if (!ftpFile.delete()) {
                log.error("Unable to delete from FTP "+ftpFile);
            }
        }
    }
    
    private void copyToFTP() {
        log.trace("Copying to FTP "+sd.msi.submissionIdentifier);

        File ftpFile = getFTPFile();
                
        //if the ftp file exists, delete it
        if (ftpFile.exists()) {
            ftpFile.delete();
        }
        
        if (!ftpFile.exists() && sampletabFile.exists()) {
            try {
                FileUtils.copy(sampletabFile, ftpFile);
                ftpFile.setLastModified(sampletabFile.lastModified());
            } catch (IOException e) {
                log.error("Unable to copy to FTP "+ftpFile, e);
            }
        }        
    }
}
