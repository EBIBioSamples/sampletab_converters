package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileGlobIterable;
import uk.ac.ebi.fgpt.sampletab.utils.FileRecursiveIterable;

import com.google.common.collect.Iterables;

public abstract class AbstractInfileDriver<T extends Callable<?>> extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filenames or globs")
    protected List<String> inputFilenames;

    @Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
    protected int threads = 0;
       
    @Option(name = "--recursive", aliases = { "-r" }, usage="recusively match filename?")
    protected boolean recursive = false;
    
    @Option(name = "--startpath", aliases = { "-s" }, usage="starting path for matching")
    protected List<File> startpaths = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());
        
    protected abstract T getNewTask(File inputFile);
    
    protected int exitCode = 0;
    
    protected void preProcess() {
        //do nothing
        //override in subclass
    }
    
    protected void postProcess() {
        //do nothing
        //override in subclass
    }
    
    protected List<String> getInputFilenames() {
        return inputFilenames;
    }
    
    protected void doMain(String[] args) {
        super.doMain(args);
        

        Iterable<File> inputFiles = null;
        for (String inputFilename : getInputFilenames()) {
            if (recursive){
                if (startpaths == null) {
                    log.info("Looking recursively for input files named "+inputFilename);
                    if (inputFiles == null) {
                        inputFiles = new FileRecursiveIterable(inputFilename, null);
                    } else {
                        inputFiles = Iterables.concat(inputFiles, new FileRecursiveIterable(inputFilename, null));
                    }
                    
                } else {
                    for (File startpath : startpaths) {
                        log.info("Looking recursively for input files named "+inputFilename+" from "+startpath);
                        if (inputFiles == null) {
                            inputFiles = new FileRecursiveIterable(inputFilename, startpath);
                        } else {
                            inputFiles = Iterables.concat(inputFiles, new FileRecursiveIterable(inputFilename, startpath));
                        }
                    }
                }
            } else {
                log.info("Looking for input files in glob "+inputFilename);   
                if (inputFiles == null) {
                    inputFiles = new FileGlobIterable(inputFilename);
                } else {
                    inputFiles = Iterables.concat(inputFiles,new FileGlobIterable(inputFilename));
                }
            }
        }

        ExecutorService pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }

        preProcess();

        if (pool != null) {
            //we are using a pool of threads to do stuff in parallel
            //create some futures and then wait for them to finish
            
            List<Future<?>> futures = new LinkedList<Future<?>>();
            for (File inputFile : inputFiles) {
                Callable<?> t = getNewTask(inputFile);
                if (t != null) {
                    Future<?> f = pool.submit(t);
                    futures.add(f);
                }
            }
            
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    //something went wrong
                    log.error("Problem processing", e);
                    exitCode = 1;
                }
            }
            
        } else {
            //we are not using a pool, its all in one
            for (File inputFile : inputFiles) {
                Callable<?> t = getNewTask(inputFile);
                if (t != null) {
                    try {
                        t.call();
                    } catch (Exception e) {
                        //something went wrong
                        log.error("Problem processing "+inputFile, e);
                        exitCode = 1;
                    }
                }
            }
        }
        
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        if (pool != null) {
            synchronized (pool) {
                pool.shutdown();
                try {
                    // allow 24h to execute. Rather too much, but meh
                    pool.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    log.error("Interupted awaiting thread pool termination", e);
                }
            }
        }
        
        postProcess();
        
        log.info("Finished reading");
        
        System.exit(exitCode);
    }
    
}
