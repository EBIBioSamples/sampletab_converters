package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dom4j.DocumentException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FileRecursiveIterable;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class ENASRACron  extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    private File outputDir;

    @Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
    private int threads = 0;

    @Option(name = "--no-conan", usage = "do not trigger conan loads")
    private boolean noconan = false;

    protected ExecutorService pool;
    private List<Future<Void>> futures = new LinkedList<Future<Void>>();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public ENASRACron() {
    	
    }
    
    protected File getOutputDir() {
        if (outputDir.exists() && !outputDir.isDirectory()) {
            System.err.println("Target is not a directory");
            System.exit(1);
        }

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        return outputDir;
    }
    
    public void doGroups(ENASRAGrouper grouper) {
        log.info("Getting sample ids by brute force");
        grouper.groupBruteForce(pool);
        log.info("Finishing getting sample ids by brute force");
    }
    
    public static void main(String[] args) {
        new ENASRACron().doMain(args);
    }
    
    @Override
    public void doMain(String[] args){
        super.doMain(args);

        pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }
        
        //first get a map of all possible submissions
        //this might take a while
        log.info("Getting groups...");
        ENASRAGrouper grouper = new ENASRAGrouper();
        doGroups(grouper);
        log.info("Got groups...");
        
        Collection<String> toDelete = getDeletions(grouper);
        
        processUpdates(grouper);
        
        processDeletions(toDelete);
        
        if (pool != null) {
            //wait for threading to finish
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    //something went wrong
                    log.error("problem processing update", e);
                }
            }
            
            // close the pool to tidy it all up
            // must synchronize on the pool object
            synchronized (pool) {
                log.info("shutting down pool");
                pool.shutdown();
                try {
                    // allow 24h to execute. Rather too much, but meh
                    pool.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    log.error("Interuppted awaiting thread pool termination", e);
                }
            }
        }
    }
   
    
    
    private void processUpdates(ENASRAGrouper grouper) {

        log.info("Processing updates");
        
        //process updates
        ENASRAWebDownload downloader = new ENASRAWebDownload();
        for(String key : grouper.groups.keySet()) {
            String submissionID = "GEN-"+key;
            log.info("checking "+submissionID);
            File outsubdir = SampleTabUtils.getSubmissionDirFile(submissionID);
            boolean changed = false;
            
            outsubdir = new File(outputDir.toString(), outsubdir.toString());
            try {
                changed = downloader.downloadXML(key, outsubdir);
            } catch (IOException e) {
                log.error("Problem downloading samples of "+key, e);
                continue;
            } catch (DocumentException e) {
                log.error("Problem downloading samples of "+key, e);
                continue;
            }
            
            for (String sample : grouper.groups.get(key)) {
                try {
                    changed |= downloader.downloadXML(sample, outsubdir);
                } catch (IOException e) {
                    log.error("Problem downloading sample "+sample, e);
                    continue;
                } catch (DocumentException e) {
                    log.error("Problem downloading sample "+sample, e);
                    continue;
                }
            }
            
            //mark it as changed if the target doesn't exist
            File sampletabPre = new File(outsubdir, "sampletab.pre.txt");
            changed |= !sampletabPre.exists();
            
            if (changed) {
                //process the subdir
                log.info("updated "+submissionID);
                
                Callable<Void> task = new ENASRAUpdateCallable(outsubdir, key, grouper.groups.get(key), !noconan);
                if (pool == null) {
                    //no threading
                    try {
                        task.call();
                    } catch (Exception e) {
                        //something went wrong
                        log.error("problem processing update of "+key, e);
                    }
                } else {
                    //with threading
                    Future<Void> f = pool.submit(task);
                    futures.add(f);
                }
            }
        }
        
        log.info("Finished processing updates");
    }
    
    protected Collection<String> getDeletions(ENASRAGrouper grouper) {
    
        log.info("Checking deletions");
        //also get a set of existing submissions to delete
        Set<String> toDelete = new HashSet<String>();
        for (File sampletabpre : new FileRecursiveIterable("sampletab.pre.txt", new File(getOutputDir(), "sra"))) {
            //get submission identifier based on parent directory
            File subdir = sampletabpre.getParentFile();
            String subId = subdir.getName();
            //strip off GEN-
            if (subId.startsWith("GEN-")) {
                subId = subId.substring(4);
            }
            if (!grouper.groups.containsKey(subId)) {
                toDelete.add(subId);
            }
        }
        log.info("Finished checking deletions");
        return toDelete;
    }
    
    private void processDeletions(Collection<String> toDelete) {

        //process deletes
        for (String submissionID : toDelete) {
            File sampletabpre = new File(outputDir.toString(), SampleTabUtils.getSubmissionDirFile(submissionID).toString());
            sampletabpre = new File(sampletabpre, "sampletab.pre.txt");
            try {
                SampleTabUtils.releaseInACentury(sampletabpre);
            } catch (IOException e) {
                log.error("problem making "+sampletabpre+" private", e);
                continue;
            } catch (ParseException e) {
                log.error("problem making "+sampletabpre+" private", e);
                continue;
            }
            //trigger conan, if appropriate
            if (!noconan) {
                if (pool != null) {
                    Future<Void> f = pool.submit(new PrivatizeCallable(submissionID));
                    futures.add(f);
                } else {
                    try {
                        ConanUtils.submit(submissionID, "BioSamples (other)");
                    } catch (IOException e) {
                        log.error("problem making "+submissionID+" private through Conan", e);
                    }
                }
            }
        }
    }
    
    private class PrivatizeCallable implements Callable<Void> {

        private final String submissionID;
        
        public PrivatizeCallable(String submissionID) {
            this.submissionID = submissionID;
        }
        @Override
        public Void call() throws Exception {
            try {
                ConanUtils.submit(submissionID, "BioSamples (other)");
            } catch (IOException e) {
                log.error("problem making "+submissionID+" private through Conan", e);
            }
            return null;
        }
        
    }
}
