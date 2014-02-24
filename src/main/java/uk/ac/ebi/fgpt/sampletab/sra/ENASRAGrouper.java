package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;

public class ENASRAGrouper {

    public final ConcurrentMap<String, Set<String>> groups = new ConcurrentHashMap<String, Set<String>>();
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public void groupBruteForce(ExecutorService pool) {
        populate("DRS", 0,  10000, pool);
        populate("SRS", 0, 600000, pool);
        populate("ERS", 0, 500000, pool);
        //no longer a need to close the pool since it is using callables with a specific future, rather than runnables
    }
    
    /**
     * Fills the groups attribute with samples. Will process it in parallel if pool is not null.
     * @param prefix
     * @param minCount
     * @param maxCount
     * @param pool
     */
    protected void populate(String prefix, int minCount, int maxCount, ExecutorService pool) {
        if (pool == null) {
            for (int i = minCount; i < maxCount; i++) {
                String sampleId = String.format("%1$s%2$06d", prefix, i);
                
                Callable<Collection<String>> t = new GroupCallable(sampleId);
                try {
                    t.call();
                    //no need to do anything with the return value since it is already added to the groups attribute
                } catch (Exception e) {
                    log.error("Problem getting group of sample "+sampleId, e);
                    throw new RuntimeException(e);
                }
            }
        } else {
            Collection<Future<Collection<String>>> futures = new ArrayList<Future<Collection<String>>>();
            for (int i = minCount; i < maxCount; i++) {
                String sampleId = String.format("%1$s%2$06d", prefix, i);
                
                Callable<Collection<String>> t = new GroupCallable(sampleId);
                Future<Collection<String>> f = pool.submit(t);
                futures.add(f);
            }    
            for (Future<Collection<String>> f : futures) {
                try {
                    f.get();
                    //no need to do anything with the return value since it is already added to the groups attribute
                } catch (InterruptedException e) {
                    log.error("Problem getting group of sample", e);
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    log.error("Problem getting group of sample", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
    /**
     * This groups a provided list of sample ids into the groups attribute. At the end, the groups
     * will contain all their associated samples, not just those provided. If pool is not null, it will be
     * used to process the sample ids in parallel.
     * @param sampleIds
     * @param pool
     */
    public void groupSampleIds(Collection<String> sampleIds, ExecutorService pool){

        if (pool == null) {
            for (String sampleId : sampleIds) {                
                Callable<Collection<String>> t = new GroupCallable(sampleId);
                try {
                    t.call();
                    //no need to do anything with the return value since it is already added to the groups attribute
                } catch (Exception e) {
                    log.error("Problem getting group of sample "+sampleId, e);
                    throw new RuntimeException(e);
                }
            }
        } else {
            Collection<Future<Collection<String>>> futures = new ArrayList<Future<Collection<String>>>();
            for (String sampleId : sampleIds) {                
                Callable<Collection<String>> t = new GroupCallable(sampleId);
                Future<Collection<String>> f = pool.submit(t);
                futures.add(f);
            }    
            for (Future<Collection<String>> f : futures) {
                try {
                    f.get();
                    //no need to do anything with the return value since it is already added to the groups attribute
                } catch (InterruptedException e) {
                    log.error("Problem getting group of sample", e);
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    log.error("Problem getting group of sample", e);
                    throw new RuntimeException(e);
                }
            }
        }
                
        //now ensure groups attribute contain ALL sample ids, not just those that have been updated
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
    
    /**
     * This allows for threading of queries against the SRA XML API. For a provided sample ID, it 
     * retrieves associated group ids. These group ids are either direct studies, indirect studies from the same submission, or 
     * fallback to the submission.
     * @author faulcon
     *
     */
    protected class GroupCallable implements Callable<Collection<String>> {
        private final String sampleId;
        
        private Logger log = LoggerFactory.getLogger(getClass());
    
        public GroupCallable(String sampleId) {
            this.sampleId = sampleId;
        }

        @Override
        public Collection<String> call() throws Exception {
            Collection<String> studyIds = null;
            studyIds = getGroupIdentifiers(sampleId);
            
            if (studyIds != null){
                if (studyIds.size()== 0) {
                    throw new RuntimeException("Unable to find any study ids for sample "+sampleId);
                } else {
                    for (String groupId : studyIds) {
                        Set<String> set = groups.get(groupId);
                        //make sure this is multithreadable with other callables being run at the same time
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
            return studyIds;
        }
        
        /**
         * use the SRA XML API to discover group identifiers for a sample.
         * 
         * should always return at least one group identifier; either direct studies, indirect studies from the same submission, or 
         * fallback to the submission.
         * 
         * @param sampleId
         * @return
         * @throws DocumentException
         * @throws IOException
         */
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
