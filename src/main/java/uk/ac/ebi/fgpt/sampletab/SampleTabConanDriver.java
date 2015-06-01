package uk.ac.ebi.fgpt.sampletab;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for bulk-submitting files to conan
 * 
 * 
 * @author faulcon
 *
 */
public class SampleTabConanDriver extends AbstractInfileDriver<SampleTabConanRunnable> {
    
    private Logger log = LoggerFactory.getLogger(getClass());

    
    public SampleTabConanDriver() {
        super();
    }
    
        
    public static void main(String[] args) {
        new SampleTabConanDriver().doMain(args);
    }

    @Override
    protected SampleTabConanRunnable getNewTask(File inputFile) {
        File subdir = inputFile.getAbsoluteFile().getParentFile();
        String submissionID = subdir.getName();
        
        File sampletabpre = new File(subdir, "sampletab.pre.txt");
        File sampletab = new File(subdir, "sampletab.txt");
        File sampletabtoload = new File(subdir, "sampletab.toload.txt");
        
        if (!sampletab.exists() || sampletabpre.lastModified() > sampletab.lastModified()) {
            return new SampleTabConanRunnable(submissionID, "BioSamples (other)", 0);
        } else if (!sampletabtoload.exists() || sampletab.lastModified() > sampletabtoload.lastModified()) {
            return new SampleTabConanRunnable(submissionID, "BioSamples (other)", 1);
        }
        
        return null;
    }
}
