package uk.ac.ebi.fgpt.sampletab.pride;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

/**
 * Given a set of input PRIDE XML files, this creates a sampletab.pre.txt file.
 * 
 * It will also optionally submit that new file to conan for processing
 * 
 * @author faulcon
 *
 */
public class PRIDEXMLCallable implements Callable<Void> {

    private final Set<File> files;
    private final File outFile;
    private final String submission;
    private final boolean conan;
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public PRIDEXMLCallable(Set<File> files, String submission, boolean conan) {
        this.files = files;
        this.outFile = new File(SampleTabUtils.getSubmissionDirFile(submission), "sampletab.pre.txt");
        this.submission = submission;
        this.conan = conan;
        
    }
    
    @Override
    public Void call() throws Exception {
        PRIDEXMLToSampleTab xmlToSampleTab = new PRIDEXMLToSampleTab();
        
        xmlToSampleTab.convert(files, outFile);

        log.info("SampleTab written to "+outFile);
        
        if (conan) {
            //submit to conan
            try {
                ConanUtils.submit(submission, "BioSamples (other)");
            } catch (IOException e) {
                log.error("Problem submitting GPR-"+submission+" to conan", e);
            }
        }
        
        return null;
    }    
}
