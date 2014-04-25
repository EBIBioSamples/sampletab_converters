package uk.ac.ebi.fgpt.sampletab.other;

import java.io.File;
import java.util.concurrent.Callable;

import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SubmissionCallable implements Callable<Void> {

    private final File inputFile;
    private final boolean conan;
    
    public SubmissionCallable(File inputFile, boolean conan) {
        this.inputFile = inputFile.getAbsoluteFile();
        this.conan = conan;
    }
    
    @Override
    public Void call() throws Exception {
        String submissionID = inputFile.getParentFile().getName();
        
        File targetDirFile = SampleTabUtils.getSubmissionDirFile(submissionID);
        targetDirFile.mkdirs();
        
        File targetFile = new File(targetDirFile, "sampletab.pre.txt");
        //only continue if the target is older than the input
        if (targetFile.exists() && targetFile.lastModified() > inputFile.lastModified()) {
            return null;
        }
        
        //copy the input over the target
        FileUtils.copy(inputFile, targetFile);
        
        if (conan) {
            ConanUtils.submit(submissionID, "BioSamples (other)");
        }
        
        return null;
        //TODO flag a warning if too many new submissions are detected?
    }

}
