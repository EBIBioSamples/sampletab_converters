package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabStatusRunnable implements Runnable {
	
	private static final Date now = new Date(); 
		
	private final SampleTabSaferParser stparser = new SampleTabSaferParser();
	
	private final File inputFile;
	private final File ftpDir;
	
    private final String ageusername;
    private final String agepassword;
    private final String agehostname;
    private final URI ageHostURI;
    
    private final Set<String> tags;
    
    private final String scriptDirFilename;
	
	public Boolean shouldBePublic = false;
	public Boolean isLoaded = false;
    public Boolean isLoadUpToDate = false;
	public Boolean isOnFTP = false;
    public Boolean isFTPUpToDate = false;
    
    
    private Logger log = LoggerFactory.getLogger(getClass());
    

	public SampleTabStatusRunnable(File inputFile, File ftpDir, String ageusername, String agepassword, String agehostname, String scriptDirFilename, Set<String> tags){
		if (inputFile == null){
			throw new IllegalArgumentException("inputFile cannot be null");
		}
		if (!inputFile.exists()){
			throw new IllegalArgumentException("inputFile must exist ("+inputFile+")");
		}
		this.inputFile = inputFile;

		if (ftpDir == null){
			throw new IllegalArgumentException("ftpDir cannot be null");
		}
		if (!ftpDir.exists()){
			throw new IllegalArgumentException("ftpDir must exist ("+ftpDir+")");
		}
		this.ftpDir = ftpDir;
		
		this.ageusername = ageusername;
		this.agepassword = agepassword;
		this.agehostname = agehostname;

        URI tempURI = null;
        try {
            tempURI = new URI(agehostname);
        } catch (URISyntaxException e) {
            log.error("Invalid URI "+agehostname, e);
        }
        this.ageHostURI = tempURI;
		
        this.scriptDirFilename = scriptDirFilename;
        
        if (tags == null){
            this.tags = new HashSet<String>();
        } else {
            this.tags = tags;
        }
	}

	public SampleTabStatusRunnable(File inputFile, String ftpDirFilename, String ageusername, String agepassword, String agehostname, String scriptDirFilename, Set<String> tags){
		this(inputFile, new File(ftpDirFilename), ageusername, agepassword, agehostname, scriptDirFilename, tags);
	}
	
	public void run() {
		
    	//each input can in one the following states:
    	//  currently public and should stay public <- do nothing
    	//  currently public and should be private <- the bad state
    	//  currently private and should be public
    	//  currently private and should stay private <- do nothing
    	//  currently not up to date and should be public
    	//  currently not up to date and should be private
	    		
    	SampleData sd = null;
    	File sampletabFile = new File(inputFile, "sampletab.txt");
        if (!sampletabFile.exists()){
            log.error(sampletabFile+" does not exist");
            return;
        }
        if (sampletabFile.isDirectory()){
            log.error(sampletabFile+" is a directory");
            return;
        }
        try {
            sd = stparser.parse(sampletabFile);
        } catch (ParseException e){
            log.error("Unable to parse file "+inputFile, e);
        	return;
        }
    	
        //calculate shouldBePublic
        //use release date inside file
    	if (sd != null){
    		if (sd.msi.submissionReleaseDate == null || sd.msi.submissionReleaseDate.before(now)){
        		//should be public
    			shouldBePublic = true;
    		} else if (sd.msi.submissionReleaseDate.after(now)){
        		//should be private
    			shouldBePublic = false;
    		}
    	}
    	
    	//calculate isLoaded
    	File loadDir = new File(inputFile, "load");
    	File sucessFile = new File(loadDir, inputFile.getName()+".SUCCESS."+ageHostURI.getHost());
        File ageDir = new File(inputFile, "age");
        File ageFile = new File(ageDir, inputFile.getName()+".age.txt");
    	//this is not a perfect check - ideally this should be an API query for last load date
        log.debug("Checking existance of "+sucessFile);
        if (!sucessFile.exists()){
            isLoaded = false;
        } else {
            isLoaded = true;
            if (ageFile.lastModified() > sucessFile.lastModified()){
                isLoadUpToDate = false;
            } else {
                isLoadUpToDate = true;
            }
        }
            	
        //calculate isPublic
    	File ftpSubDir = new File(ftpDir, SampleTabUtils.getPathPrefix(inputFile.getName())); 
		File ftpSubSubDir = new File(ftpSubDir, inputFile.getName());
		File ftpFile = new File(ftpSubSubDir, "sampletab.txt");
    	if (ftpFile.exists()){
    		isOnFTP = true;
            if (sampletabFile.lastModified() > ftpFile.lastModified()){
                isFTPUpToDate = false;
            } else {
                isFTPUpToDate = true;
            }
    	} else {
    		isOnFTP = false;
    	}
    	
    	log.debug(inputFile.getName()+" "+shouldBePublic+" "+isLoaded+" "+isLoadUpToDate+" "+isOnFTP+" "+isFTPUpToDate);
    	
    	
    	//now we have the information, determine what we need to do
    	
    	if (shouldBePublic){
    	    if (isLoaded && this.tags.contains("Security:Private")){
    	        //remove private tag
                removePrivateTag();
    	    }
    	    if (!isLoaded || !isLoadUpToDate){
    	        //reload
    	        reload(shouldBePublic);
    	    }
    	    if (!isOnFTP || !isFTPUpToDate){
    	        //copy to FTP
    	        copyToFTP();
    	    }
    	} else if (!shouldBePublic) {
            if (isLoaded && !this.tags.contains("Security:Private")){
                //add private tag
                addPrivateTag();
            }
            if (isOnFTP){
                //remove from FTP
                removeFromFTP();
            }
    	}
	}
	
	private void removePrivateTag(){
        if (ageusername == null){
            log.info("Skipping removing private tag "+inputFile.getName());
            return;
        }
        if (!this.tags.contains("Security:Private")){
            return;
        }
        
	    log.info("Removing private tag "+inputFile.getName());	    
	    
        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "TagControl.sh");
        
        SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        File logfile = new File(inputFile, "tag_"+simpledateformat.format(new Date())+".log");

        String command = scriptFile.getAbsolutePath() 
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\"" 
            + " -r Security:Private"
            + " -sbm "+inputFile.getName();

        ProcessUtils.doCommand(command, logfile);
	}
    
    private void addPrivateTag(){
        if (ageusername == null){
            log.info("Skipping adding private tag "+inputFile.getName());
            return;
        }
        if (this.tags.contains("Security:Private")){
            return;
        }
        
        log.info("Adding private tag "+inputFile.getName());
        
        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "TagControl.sh");
        
        SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        File logfile = new File(inputFile, "tag_"+simpledateformat.format(new Date())+".log");

        String command = scriptFile.getAbsolutePath() 
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\"" 
            + " -a Security:Private"
            + " -sbm "+inputFile.getName();

        ProcessUtils.doCommand(command, logfile);
    }    
    private void reload(boolean isPublic){
     
        if (ageusername == null){
            log.info("Skipping reloading "+inputFile.getName());
            return;
        }
        
        log.info("Reloading in database "+inputFile.getName());
        
        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "AgeTab-Loader.sh");
        File loadDir = new File(inputFile, "load");
        File ageDir = new File(inputFile, "age");

        SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        File logfile = new File(inputFile, "load_"+simpledateformat.format(new Date())+".log");
        
        String command = scriptFile.getAbsolutePath() 
            + " -s -m -i -e"
            + " -o \""+loadDir.getAbsolutePath()+"\""
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\"";
        if (!isPublic){
            command = command+" -a Security:Private";
        }
        command = command+" \""+ageDir.getAbsolutePath()+"\""; 
        
        ProcessUtils.doCommand(command, logfile);
        
        //move the server response file
        File serverResponseOrigSuccess = new File(loadDir, inputFile.getName()+".SUCCESS");
        File serverResponseOrigError = new File(loadDir, inputFile.getName()+".ERROR");
        File serverResponseNewSuccess = new File(loadDir, inputFile.getName()+".SUCCESS."+ageHostURI.getHost());
        File serverResponseNewError = new File(loadDir, inputFile.getName()+".ERROR."+ageHostURI.getHost());
        //this could probably have more checks, but how would we handle fails and differently?
        serverResponseNewSuccess.delete();
        serverResponseNewError.delete();
        if (serverResponseOrigSuccess.exists()){
            serverResponseOrigSuccess.renameTo(serverResponseNewSuccess);
        }
        if (serverResponseOrigError.exists()){
            serverResponseOrigError.renameTo(serverResponseNewError);
        }
    }
    
    private File getFTPFile(){
        File ftpSubDir = new File(ftpDir, SampleTabUtils.getPathPrefix(inputFile.getName())); 
        File ftpSubSubDir = new File(ftpSubDir, inputFile.getName());
        File ftpFile = new File(ftpSubSubDir, "sampletab.txt");
        return ftpFile;
        
    }
    
    private void removeFromFTP(){
        log.info("Removing from FTP "+inputFile.getName());
        File ftpFile = getFTPFile();
        if (ftpFile.exists()){
            if (!ftpFile.delete()){
                log.error("Unable to delete from FTP "+ftpFile);
            }
        }
    }
    
    private void copyToFTP(){
        log.info("Copying to FTP "+inputFile.getName());

        File ftpFile = getFTPFile();
        
        File sampletabFile = new File(inputFile, "sampletab.txt");
        
        //if the ftp file exists, delete it
        if (ftpFile.exists()){
            ftpFile.delete();
        }
        
        if (!ftpFile.exists() && sampletabFile.exists()){
            try {
                FileUtils.copy(sampletabFile, ftpFile);
                ftpFile.setLastModified(sampletabFile.lastModified());
            } catch (IOException e) {
                log.error("Unable to copy to FTP "+ftpFile, e);
            }
        }        
    }
    
    private void removeToLoad(){
        log.info("Removing toLoad "+inputFile.getName());
        File toLoad = new File(inputFile, "sampletab.toload.txt");
        if (toLoad.exists()){
            if (!toLoad.delete()){
                log.error("Unable to delete file "+toLoad);
            }
        }
    }
    
    private Set<String> getTags(){
        if (ageusername == null){
            log.info("Skipping getting tags "+inputFile.getName());
            return null;
        }

        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "TagControl.sh");
        
        String command = scriptFile.getAbsolutePath() 
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\""
            + " -l -sbm "+inputFile.getName();

        ArrayList<String> bashcommand = new ArrayList<String>();
        bashcommand.add("/bin/bash");
        bashcommand.add("-c");
        bashcommand.add(command);

        ProcessBuilder pb = new ProcessBuilder();
        pb.command(bashcommand);
        pb.redirectErrorStream(true);// merge stderr to stdout
        Process p = null;
        Set<String> tags = new HashSet<String>();
        try {
            p = pb.start();
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                String tag = parts[2];
                tags.add(tag);
            }
            synchronized (p) {
                p.waitFor();
            }
        } catch (IOException e) {
            log.error("Error running " + command, e);
            return null;
        } catch (InterruptedException e) {
            log.error("Error running " + command, e);
            return null;
        }
        return tags;
    }
}
