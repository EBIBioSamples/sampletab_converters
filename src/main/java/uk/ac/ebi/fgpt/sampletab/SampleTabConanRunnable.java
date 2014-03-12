package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;

public class SampleTabConanRunnable implements Callable<Void> {
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private final String submissionIdentifier;
    private final String pipeline;
    
    public SampleTabConanRunnable(String submissionIdentifier, String pipeline) {
        this.submissionIdentifier = submissionIdentifier;
        this.pipeline = pipeline;
    }
    
    @Override
    public Void call() throws Exception {
        try {
            ConanUtils.submit(submissionIdentifier, pipeline);
        } catch (IOException e) {
            log.error("Problem submitting "+submissionIdentifier+" to "+pipeline);
        }
        return null;
    }

}
