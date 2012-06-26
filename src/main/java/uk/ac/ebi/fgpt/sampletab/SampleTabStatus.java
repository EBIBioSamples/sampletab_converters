package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabStatusRunnable;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabStatus {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="FTP", usage = "ftp directory")
    private String ftpDirFilename;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new SampleTabStatus().doMain(args);
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
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        

        log.debug("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
        }
        log.info("Found " + inputFiles.size() + " input files");
        //TODO no duplicates
        Collections.sort(inputFiles);

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
        for (File inputFile : inputFiles) {
        	if (!inputFile.isDirectory()){
        		inputFile = inputFile.getAbsoluteFile().getParentFile();
        	}
    		if (inputFile == null){
    			continue;
    		}
    		
            Runnable t = new SampleTabStatusRunnable(inputFile, ftpDirFilename);
            if (threaded){
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
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        
        //remove from FTP
        for(File inputFile : SampleTabStatusRunnable.toRemoveFromFTP){
    		File ftpDir = new File(ftpDirFilename);
        	File ftpSubDir = new File(ftpDir, SampleTabUtils.getPathPrefix(inputFile.getName())); 
    		File ftpSubSubDir = new File(ftpSubDir, inputFile.getName());
    		File ftpFile = new File(ftpSubSubDir, "sampletab.txt");
    		
    		if (ftpFile.exists()){
	    		if (!ftpFile.delete()){
	    			log.error("Unable to delete from FTP "+ftpFile);
	    		}
    		}
        }
        
        //remove from database
        //TODO finish
        
        
        //copy to FTP
        for(File inputFile : SampleTabStatusRunnable.toCopyToFTP){
    		File ftpDir = new File(ftpDirFilename);
        	File ftpSubDir = new File(ftpDir, SampleTabUtils.getPathPrefix(inputFile.getName())); 
    		File ftpSubSubDir = new File(ftpSubDir, inputFile.getName());
    		File ftpFile = new File(ftpSubSubDir, "sampletab.txt");
    		
    		File sampletabFile = new File(inputFile, "sampletab.txt");
    		
    		if (!ftpFile.exists() && sampletabFile.exists()){
    			try {
					FileUtils.copy(sampletabFile, ftpFile);
				} catch (IOException e) {
	    			log.error("Unable to copy to FTP "+ftpFile);
					e.printStackTrace();
				}
    		}
        }
        
        //add to database
        //TODO finish
        
    }
}
