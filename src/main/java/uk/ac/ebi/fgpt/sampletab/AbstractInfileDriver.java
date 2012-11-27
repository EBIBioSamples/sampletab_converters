package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private List<String> inputFilenames;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
       
    private Logger log = LoggerFactory.getLogger(getClass());
        
    protected abstract T getNewTask(File inputFile);
    
    protected void doMain(String[] args){
        super.doMain(args);
        

        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            log.info("Looking for input files "+inputFilename);
            inputFiles = FileUtils.getMatchesGlob(inputFilename);
        }
        log.info("Found " + inputFiles.size() + " input files");
        Collections.sort(inputFiles);

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

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
        log.info("Finished reading");
    }
    
}
