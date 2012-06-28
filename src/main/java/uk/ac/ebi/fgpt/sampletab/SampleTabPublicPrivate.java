package uk.ac.ebi.fgpt.sampletab;

import java.io.File;

import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabPublicPrivate {

    private final File ftpPath;
    
    public SampleTabPublicPrivate(File ftpPath){
        this.ftpPath = ftpPath;
        if (this.ftpPath.exists()){
            throw new IllegalArgumentException();
        }
    }
    
    public SampleTabPublicPrivate(String ftpPath){
        this(new File(ftpPath));
    }
    
    public boolean isPublic(String submission){
        //check if its on the ftp
        File subdir = new File(ftpPath, SampleTabUtils.getPathPrefix(submission));
        if (!subdir.exists()){
            return false;
        }
        File subsubdir = new File(subdir, submission);
        if (!subsubdir.exists()){
            return false;
        }
        File sampletab = new File (subsubdir, "sampletab.txt");
        if (!sampletab.exists() || !sampletab.canRead()){
            return false;
        }
        
        //hasn't failed, so pass it
        return true;
    }    

    public boolean isPrivate(String submission){
        return !isPublic(submission);
    }
    
}
