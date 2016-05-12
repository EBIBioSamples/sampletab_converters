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
        
        return new SampleTabConanRunnable(submissionID, "BioSamples (other)", 0);
    }
}
