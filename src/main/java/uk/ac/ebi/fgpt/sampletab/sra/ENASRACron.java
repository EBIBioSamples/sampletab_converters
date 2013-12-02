package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FileRecursiveIterable;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class ENASRACron {

    @Option(name = "--help", aliases = { "-h" }, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    private String outputDirName;

    @Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
    private int threads = 0;

    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;
    
    @Option(name = "--no-ERAPRO", usage = "Do not get sample ids from ERA-PRO?")
    private boolean noEraPro = false;

    private Logger log = LoggerFactory.getLogger(getClass());
    private ResultSet rs = null;

    ENASRACron(){
    	
    }
    ENASRACron(ResultSet rs) {
        this.rs = rs;
    }

    public static void main(String[] args) {
        new ENASRACron().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        File outdir = new File(this.outputDirName);

        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Target is not a directory");
            System.exit(1);
            return;
        }

        if (!outdir.exists()) {
            outdir.mkdirs();
        }

        ExecutorService pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }
        
        //first get a map of all possible submissions
        //this might take a while
        log.info("Getting groups...");
        ENASRAGrouper grouper = new ENASRAGrouper(pool, noEraPro);
        
        if (noEraPro != false){
        	log.info("Getting sample ids from ERAPRO");
        	grouper.ERAPROGrouper(rs);
        }
        
        log.info("Got groups...");
        
        log.info("Checking deletions");
        //also get a set of existing submissions to delete
        Set<String> toDelete = new HashSet<String>();
        for (File sampletabpre : new FileRecursiveIterable("sampletab.pre.txt", new File(outdir, "sra"))) { 
            //TODO do this properly somehow
            File subdir = sampletabpre.getParentFile();
            String subId = subdir.getName();
            if (!grouper.groups.containsKey(subId) && !grouper.ungrouped.contains(subId)) {
                toDelete.add(subId);
            }
        }
        
        log.info("Processing updates");
        
        //restart the pool for part II        
        pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }
        List<Future<Void>> futures = new LinkedList<Future<Void>>();
        
        //process updates
        ENASRAWebDownload downloader = new ENASRAWebDownload();
        for(String key : grouper.groups.keySet()) {
            String submissionID = "GEN-"+key;
            log.info("checking "+submissionID);
            File outsubdir = SampleTabUtils.getSubmissionDirFile(submissionID);
            boolean changed = false;
            
            outsubdir = new File(outdir.toString(), outsubdir.toString());
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
            
            if (changed) {
                //process the subdir
                log.info("updated "+submissionID);
                
                Callable<Void> task = new ENASRAUpdateRunnable(outsubdir, key, grouper.groups.get(key), !noconan);
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
        
        //process deletes
        for (String submissionID : toDelete) {
            File sampletabpre = new File(outdir.toString(), SampleTabUtils.getSubmissionDirFile(submissionID).toString());
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
                    pool.execute(new PrivatizeRunnable(submissionID));
                } else {
                    try {
                        ConanUtils.submit(submissionID, "BioSamples (other)");
                    } catch (IOException e) {
                        log.error("problem making "+submissionID+" private through Conan", e);
                    }
                }
            }
        }
        
        if (pool != null) {
            //wait for threading to finish
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    //something went wrong
                    log.error("problem processing update", e);
                }
            }
            // run the pool and then close it afterwards
            // must synchronize on the pool object
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
        log.info("Finished processing updates");
    }
    
    private class PrivatizeRunnable implements Runnable {

        private final String submissionID;
        
        public PrivatizeRunnable(String submissionID) {
            this.submissionID = submissionID;
        }
        
        @Override
        public void run() {
            try {
                ConanUtils.submit(submissionID, "BioSamples (other)");
            } catch (IOException e) {
                log.error("problem making "+submissionID+" private through Conan", e);
            }
        }
        
    }
}
