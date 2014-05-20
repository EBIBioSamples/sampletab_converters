package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

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
    
    
    protected void handleDocument(Document document) {
        
        Element sampleSet = document.getRootElement();
        Element sample = XMLUtils.getChildByName(sampleSet, "BioSample");
        String accession = sample.attributeValue("accession");
        String id = sample.attributeValue("id");
        
        if (accession == null) {
            log.warn("No accession for sample "+id);
            return;
        }
        
        
        File localOutDir = new File(outDir, SampleTabUtils.getSubmissionDirPath("GNC-"+accession));
        localOutDir = localOutDir.getAbsoluteFile();
        localOutDir.mkdirs();
        File outFile = new File(localOutDir, "out.xml");


        //TODO only write out if target does not exist, 
        //TODO only write out if target is different content (not text match)
        
        log.info("writing to "+outFile);
        
        try {
            XMLUtils.writeDocumentToFile(document, outFile);
        } catch (IOException e) {
            log.error("Problem writing to "+outFile, e);
            return;
        }
    }
    
    protected void doMain(String[] args) {
        super.doMain(args);

        Date fromDate = null;
        Date toDate = null;
        try {
            fromDate = dateFormat.parse(fromDateString);
            toDate = dateFormat.parse(toDateString);
        } catch (ParseException e) {
            log.error("Unable to parse date");
            return;
        }
                

        ExecutorService pool = null;
        if (threads > 0) {
            pool = Executors.newFixedThreadPool(threads);
        }

        if (pool != null) {
            //we are using a pool of threads to do stuff in parallel
            //create some futures and then wait for them to finish
            
            LinkedList<Future<Document>> futures = new LinkedList<Future<Document>>();
            for (Integer id : new NCBIUpdateDownloader.UpdateIterable(fromDate, toDate)) {
                Callable<Document> t = new NCBIUpdateDownloader.UpdateCallable(id);
                if (t != null) {
                    Future<Document> f = pool.submit(t);
                    futures.add(f);
                }
                //limit size of future list to limit memory consumption
                while (futures.size() > 1000) {
                    Future<Document> f = futures.pop();
                    try {
                        handleDocument(f.get());
                    } catch (Exception e) {
                        //something went wrong
                        log.error("Problem processing", e);
                    }
                }
            }
            
            for (Future<Document> f : futures) {
                try {
                    handleDocument(f.get());
                } catch (Exception e) {
                    //something went wrong
                    log.error("Problem processing", e);
                }
            }
            
        } else {
            //we are not using a pool, its all in one
            for (Integer id : new NCBIUpdateDownloader.UpdateIterable(fromDate, toDate)) {
                Callable<Document> t = new NCBIUpdateDownloader.UpdateCallable(id);
                if (t != null) {
                    try {
                        handleDocument(t.call());
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
