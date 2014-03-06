package uk.ac.ebi.fgpt.sampletab.sra;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.BioSDUtils;

public class EraProDriver extends ENASRACron {

    @Argument(required = true, index = 1, metaVar = "STARTDATE", usage = "Start date as YYYY/MM/DD")
    protected String minDateString;

    @Argument(required = false, index = 2, metaVar = "ENDDATE", usage = "End date as YYYY/MM/DD")
    protected String maxDateString;
   
    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new EraProDriver().doMain(args);
    }
    
    public void doGroups(ENASRAGrouper grouper) {
        log.info("Getting sample ids from ERA-PRO");
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date minDate = null;
        Date maxDate = null;
        try {
            minDate = formatter.parse(minDateString);
            if (maxDateString != null) { 
                maxDate = formatter.parse(maxDateString);
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        
        Collection<String> sampleIds = EraProManager.getInstance().getUpdatesSampleId(minDate, maxDate);
        
        grouper.groupSampleIds(sampleIds, pool);

        //at this point we have worked out which groups need to be updated
        //we still need to to private -> public 
        //(public -> private is in the getDeletions method)
                
        log.info("Finishing getting sample ids from ERA-PRO");   
    }
    
    @Override
    protected Collection<String> getDeletions(ENASRAGrouper grouper) {
        log.info("Checking deletions");
        
        //Collection<String> publicSamples = eraProManager.getPublicSamples();
        Collection<String> privateSamples = EraProManager.getInstance().getPrivateSamples();
        
        //for each private sample, check against the biosamples API and see if it is accessible
        Set<String> toDelete = new HashSet<String>();

        Collection<Future<String>> futures = new ArrayList<Future<String>>();
        if (pool == null) {
            for (String sampleID : privateSamples){
                Callable<String> c = new TestPublicCallable(sampleID);
                try {
                    if (c.call() != null) {
                        toDelete.add(sampleID);
                    }
                } catch (Exception e) {
                    log.error("Problem testing public status of "+sampleID, e);
                }
            }            
        } else {
            for (String sampleID : privateSamples){
                Callable<String> c = new TestPublicCallable(sampleID);
                Future<String> f = pool.submit(c);
                futures.add(f);
            }
            for (Future<String> f : futures) {
                String sampleID;
                try {
                    sampleID = f.get();
                    if (sampleID != null) {
                        log.info("adding "+sampleID);
                        toDelete.add(sampleID);
                    }
                } catch (InterruptedException e) {
                    log.error("Problem testing public status", e);
                } catch (ExecutionException e) {
                    log.error("Problem testing public status", e);
                }
            }
        }
        
        for (String sampleID : privateSamples){
            log.trace("checking "+sampleID);
            if (BioSDUtils.isBioSDAccessionPublic(sampleID)) {
                log.info("adding "+sampleID);
                toDelete.add(sampleID);
            }
        }
        log.info("Finished checking deletions, found "+toDelete.size());
        return toDelete;
    }
    
    private class TestPublicCallable implements Callable<String> {

        private String sampleID;
        
        public TestPublicCallable(String sampleID) {
            this.sampleID = sampleID;
        }
        
        @Override
        public String call() throws Exception {
            if (BioSDUtils.isBioSDAccessionPublic(sampleID)) {
                return sampleID;
            } else {
                return null;
            }
        }
        
    }
}
