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

public class PRIDEXMLCallable implements Callable<Void> {

    private final Set<File> files;
    private final File outFile;
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public PRIDEXMLCallable(Set<File> files, File outFile) {
        this.files = files;
        this.outFile = outFile;
    }
    
    @Override
    public Void call() throws Exception {
        PRIDEXMLToSampleTab xmlToSampleTab = new PRIDEXMLToSampleTab();
        
        xmlToSampleTab.convert(files, outFile);

        log.info("SampleTab written to "+outFile);
        return null;
    }    
}
