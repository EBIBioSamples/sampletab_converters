package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public abstract class AbstractInfileDriver<T extends Runnable> extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filenames or globs")
    protected List<String> inputFilenames;

    @Option(name = "--threaded", aliases = { "-t" }, usage = "use multiple threads?")
    private boolean threaded = false;
       
    @Option(name = "--recursive", aliases = { "-r" }, usage="recusively match filename?")
    private boolean recusive = false;
    
    @Option(name = "--startpath", aliases = { "-s" }, usage="starting path for matching")
    private File startpath;
    
    private Logger log = LoggerFactory.getLogger(getClass());
        
    protected abstract T getNewTask(File inputFile);
    
    protected void preProcess(){
        //do nothing
        //override in subclass
    }
    
    protected void postProcess(){
        //do nothing
        //override in subclass
    }
    
    
    protected void doMain(String[] args){
        super.doMain(args);
        

        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            log.info("Looking for input files "+inputFilename);
            if (recusive){
                inputFiles.addAll(FileUtils.getRecursiveFiles(inputFilename, startpath));
            } else {
                inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
            }
        }
        log.info("Found " + inputFiles.size() + " input files");
        Collections.sort(inputFiles);
        
        //remove duplicates
        Set<File> inputFileSet = new HashSet<File>();
        inputFileSet.addAll(inputFiles);
        inputFiles.clear();
        inputFiles.addAll(inputFileSet);
        
        //put into reliable order
        Collections.sort(inputFiles);

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        preProcess();
        
        for (File inputFile : inputFiles) {
            Runnable t = getNewTask(inputFile);
            if (threaded) {
                pool.execute(t);
            } else {
                t.run();
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
                log.error("Interupted awaiting thread pool termination", e);
            }
        }
        
        postProcess();
        
        log.info("Finished reading");
    }
    
}
