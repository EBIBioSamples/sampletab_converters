package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;

public class ENASRAGrouper {

    public final ConcurrentMap<String, Set<String>> groups = new ConcurrentHashMap<String, Set<String>>();
    
    public final Set<String> ungrouped = Collections.synchronizedSet(new HashSet<String>());
    
    public final Set<String> failed = Collections.synchronizedSet(new HashSet<String>());
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public void groupBruteForce(ExecutorService pool) {
        populate("DRS", 0, 5000, pool);
        populate("SRS", 0, 500000, pool);
        populate("ERS", 0, 250000, pool);
        
        if (pool != null) {
            synchronized (pool) {
                pool.shutdown();
                try {
                    // allow 24h to execute. Rather too much, but meh
                    pool.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    log.error("Interuppted awaiting thread pool termination", e);
                }
            }
        }
        
        //some of them may have failed
        //try again in case it was a transient fail
        //pool is already shutdown, so single-thread this part
        Set<String> failedOnce = new HashSet<String>();
        failedOnce.addAll(failed);
        failed.clear();
        for (String sampleId : failedOnce) {
            Runnable t = new GroupRunnable(sampleId);
            t.run();
        }
        //some of them may still have failed
        //abandon hope
        
    }
    
    public void groupSampleIds(Collection<String> sampleIds){
		for (String sampleId : sampleIds) {
			Runnable t = new GroupRunnable(sampleId);
			//TODO use a pool
			t.run();
		}
		//now ensure groups contain ALL sample ids, not just those that have been updated
		//TODO use a pool
		for (String groupId : groups.keySet()) {
		    if (groupId.matches("[EDS]RP[0-9]+")) {
		        try {
                    groups.get(groupId).addAll(ENAUtils.getSamplesForStudy(groupId));
                } catch (DocumentException e) {
                    log.error("Problem getting samples of "+groupId, e);
                } catch (IOException e) {
                    log.error("Problem getting samples of "+groupId, e);
                }
		    } else if (groupId.matches("[EDS]RA[0-9]+")) {
                try {
                    groups.get(groupId).addAll(ENAUtils.getSamplesForSubmission(groupId));
                } catch (DocumentException e) {
                    log.error("Problem getting samples of "+groupId, e);
                } catch (IOException e) {
                    log.error("Problem getting samples of "+groupId, e);
                }
            } else {
                log.warn("Unrecognized group ID "+groupId);
            }
		}
    }
    
    protected void populate(String prefix, int minCount, int maxCount, ExecutorService pool) {
        for (int i = minCount; i < maxCount; i++) {
            String sampleId = String.format("%1$s%2$06d", prefix, i);
            
            Runnable t = new GroupRunnable(sampleId);
            if (pool == null) {
                t.run();
            } else {
                pool.execute(t);
            }
            
        }        
    }
    
    protected class GroupRunnable implements Runnable {
        private final String sampleId;
        
        private Logger log = LoggerFactory.getLogger(getClass());
    
        public GroupRunnable(String sampleId) {
            this.sampleId = sampleId;
        }
        
        @Override
        public void run() {
            Collection<String> studyIds = null;
            try {
                studyIds = getGroupIdentifiers(sampleId);
            } catch (DocumentException e){
                log.error("Unable to process "+sampleId, e);
                failed.add(sampleId);
                return;
            } catch (IOException e) {
                log.error("Unable to process "+sampleId, e);
                failed.add(sampleId);
                return;
            }
            
            if (studyIds != null){
                if (studyIds.size()== 0) {
                    ungrouped.add(sampleId);
                } else {
                    for (String groupId : studyIds) {
                        
                        
                        Set<String> set = groups.get(groupId);
                        if (set == null) {
                            final Set<String> value = Collections.synchronizedSet(new HashSet<String>());
                            set = groups.putIfAbsent(groupId, value);
                            if (set == null) {
                                set = value;
                            }
                        }
                        set.add(sampleId);
                    }
                }
            }
            
        }
        
        private Collection<String> getGroupIdentifiers(String sampleId) throws DocumentException, IOException {
            
            Integer i = new Integer(sampleId.substring(3, sampleId.length()));
            if (i % 1000 == 0){
                log.info("processing "+sampleId);
            }
            
            Set<String> studyIDs = ENAUtils.getStudiesForSample(sampleId);
            
            if (studyIDs.size() == 0) {
                //did not find any studies directly
                //try indirect via submission
                //only add one study this way
                Collection<String> submissionIds = ENAUtils.getSubmissionsForSample(sampleId);
                for (String submissionId : submissionIds) {
                    Collection<String> studiesInSubmission = ENAUtils.getStudiesForSubmission(submissionId);
                    if (studiesInSubmission.size() == 1) {
                        studyIDs.addAll(studiesInSubmission);
                    }
                }
                
                if (studyIDs.size() == 0) {
                    //if there are still no study ids, use submission ids
                    studyIDs.addAll(submissionIds);
                }
            }
            return studyIDs;
        }
    
    }
}
