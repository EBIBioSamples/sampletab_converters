package uk.ac.ebi.fgpt.sampletab.guixml;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;

import net.sf.saxon.s9api.SaxonApiException;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ParserCallable;

@SuppressWarnings("restriction")
public class GUIXMLDriver extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
    private String outputFilename;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;
    
    private final List<File> inputFiles = new ArrayList<File>();
    private final List<Future<SampleData>> futureSampleDatas = new ArrayList<Future<SampleData>>();
    private int index = 0;
    private final int cache = 15;
    
    private final int nothreads = Runtime.getRuntime().availableProcessors();
    private final ExecutorService pool = Executors.newFixedThreadPool(nothreads);

    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new GUIXMLDriver().doMain(args);
    }
    
    private void setIndex(int index){
        this.index = index;
        
        //add new futures to be processed
        int targetFutures = index+cache;
        if (targetFutures > inputFiles.size()){
            targetFutures = inputFiles.size();
        }
        while (futureSampleDatas.size() < targetFutures){
            File inputFileFuture = inputFiles.get(futureSampleDatas.size());
            Callable<SampleData> call = new ParserCallable(inputFileFuture);
            futureSampleDatas.add(pool.submit(call));
        }
    }
    
    private SampleData getNext(){
        SampleData sd = null;
        while (index < inputFiles.size() && sd == null){
            Future<SampleData> futureSampleData = futureSampleDatas.get(index);
            File inputFile = inputFiles.get(index);
            try {
                sd = futureSampleData.get();
            } catch (InterruptedException e) {
                log.error("Unable to process "+inputFile, e);
            } catch (ExecutionException e) {
                log.error("Unable to process "+inputFile, e);
            }
            setIndex(index+1);
        }
        
        return sd;
    }
    
    
    public void doMain(String[] args) {

        super.doMain(args);

        log.info("Looking for input files");
        for (String inputFilename : inputFilenames){
            inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
        }
        log.info("Found " + inputFiles.size() + " input files");
        
        //remove duplicates
        Set<File> inputFileSet = new HashSet<File>();
        inputFileSet.addAll(inputFiles);
        inputFiles.clear();
        inputFiles.addAll(inputFileSet);
        
        //put into reliable order
        Collections.sort(inputFiles);
        
        File outputFile = new File(outputFilename);
        outputFile = outputFile.getAbsoluteFile();
        if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()){
            log.error("Unable to make directories for "+outputFile);
            return;
        }
        
        setIndex(0);
        
        GUIXMLOutputer outputer = new GUIXMLOutputer(outputFile);
        try {
            outputer.start();
            
            SampleData sd = null;
            sd = getNext();
            while (sd != null){
                outputer.process(sd);
                sd = getNext();
            }
            
        } catch (SaxonApiException e) {
            log.error("Error generating GUI XML", e);
        } catch (XMLStreamException e) {
            log.error("Error generating GUI XML", e);
        } finally {
            try {
                outputer.end();
            } catch (XMLStreamException e) {
                log.error("Error generating GUI XML", e);
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

}
