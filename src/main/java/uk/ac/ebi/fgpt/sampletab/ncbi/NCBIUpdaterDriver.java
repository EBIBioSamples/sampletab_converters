package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class NCBIUpdaterDriver extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="FROM", usage = "from date (yyyy/MM/dd)")
    protected String fromDateString;

    @Argument(required=true, index=1, metaVar="TO", usage = "to date (yyyy/MM/dd)")
    protected String toDateString;

    @Option(name = "-o", aliases={"--output"}, metaVar="OUTPUT", usage = "directory to write to")
    protected File outDir = null;

    @Option(name = "--threads", aliases = { "-t" }, usage = "number of additional threads")
    protected int threads = 0;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

    private Logger log = LoggerFactory.getLogger(getClass());
    
       
    protected void doMain(String[] args) {
        super.doMain(args);

        Date fromDate = null;
        Date toDate = null;
        try {
            fromDate = dateFormat.parse(fromDateString);
        } catch (ParseException e) {
            log.error("Unable to parse date "+fromDateString);
            return;
        }
        try {
            toDate = dateFormat.parse(toDateString);
        } catch (ParseException e) {
            log.error("Unable to parse date "+toDateString);
            return;
        }
                

        ExecutorService pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }

        if (pool != null) {
            //we are using a pool of threads to do stuff in parallel
            //create some futures and then wait for them to finish
            
            LinkedList<Future<Void>> futures = new LinkedList<Future<Void>>();
            for (Integer id : new NCBIUpdateDownloader.UpdateIterable(fromDate, toDate)) {
                Callable<Void> t = new NCBIUpdateDownloader.DownloadConvertCallable(id, outDir);
                if (t != null) {
                    Future<Void> f = pool.submit(t);
                    futures.add(f);
                }
                //limit size of future list to limit memory consumption
                while (futures.size() > 1000) {
                    try {
                        futures.pop().get();
                    } catch (InterruptedException e) {
                        log.error("Interupted processing", e);
                    } catch (ExecutionException e) {
                        log.error("Problem processing", e.getCause());
                    }
                }
            }
            
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    log.error("Interupted processing", e);
                } catch (ExecutionException e) {
                    log.error("Problem processing", e.getCause());
                }
            }
            
        } else {
            //we are not using a pool, its all in one
            for (Integer id : new NCBIUpdateDownloader.UpdateIterable(fromDate, toDate)) {
                Callable<Void> t = new NCBIUpdateDownloader.DownloadConvertCallable(id, outDir);
                if (t != null) {                    
                    try {
                        t.call();
                    } catch (Exception e) {
                        //something went wrong
                        log.error("Problem processing "+id, e);
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
    }
    
    public static void main(String[] args){
        new NCBIUpdaterDriver().doMain(args);
    }
}
