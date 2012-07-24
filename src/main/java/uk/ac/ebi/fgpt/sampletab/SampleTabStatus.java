package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabStatus {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="FTP", usage = "ftp directory")
    private String ftpDirFilename;

    @Argument(required=true, index=1, metaVar="SCRIPT", usage = "script directory")
    private String scriptDirFilename;

    @Argument(required=true, index=2, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
    
    @Option(name = "--no-age", usage = "")
    private boolean noAGE = false;

    @Option(name = "--agename", usage = "Age server hostname")
    private String agehostname; //default set in constructor

    @Option(name = "--ageusername", usage = "Age server username")
    private String ageusername; //default set in constructor

    @Option(name = "--agepassword", usage = "Age server password")
    private String agepassword; //default set in constructor

    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleTabStatus(){
        Properties ageProperties = new Properties();
        try {
            ageProperties.load(SampleTabStatus.class.getResourceAsStream("/age.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource age.properties");
            e.printStackTrace();
        }
        this.agehostname = ageProperties.getProperty("hostname");
        this.ageusername = ageProperties.getProperty("username");
        this.agepassword = ageProperties.getProperty("password");
    }
    
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
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        if (noAGE){
            agehostname = null;
            ageusername = null;
            agepassword = null;
        }

        log.info("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
        }
        log.info("Found " + inputFiles.size() + " input files");
        
        //no duplicates
        Set<File> inputFileSet = new HashSet<File>();
        for (File inputFile : inputFiles) {
            if (!inputFile.isDirectory()) {
                inputFile = inputFile.getAbsoluteFile().getParentFile();
            }
            if (inputFile != null) {
                inputFileSet.add(inputFile);
            }
        }
        inputFiles.clear();
        inputFiles.addAll(inputFileSet);
        
        //sort to get consistent order
        Collections.sort(inputFiles);
        
        log.info("Found " + inputFiles.size() + " input files");

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);

        startMMode();
        
        for (File inputFile : inputFiles) {
        	if (!inputFile.isDirectory()){
        		inputFile = inputFile.getAbsoluteFile().getParentFile();
        	}
    		if (inputFile == null){
    			continue;
    		}
    		
            Runnable t = new SampleTabStatusRunnable(inputFile, ftpDirFilename, ageusername, agepassword, agehostname, scriptDirFilename);
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
        
        stopMMode();
        
    }
    
    private void startMMode(){
        if (ageusername == null){
            log.info("Skipping start MMode");
            return;
        }
        
        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "MModeTool.sh");

        String command = scriptFile.getAbsolutePath() 
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\"" 
            + " -t "+(4*60*60)//4 hour timeout, just in case
            + " set" ; 

        ProcessUtils.doCommand(command, null);
    }
    
    private void stopMMode(){
        if (ageusername == null){
            log.info("Skipping stop MMode");
            return;
        }

        File scriptDir = new File(scriptDirFilename);
        File scriptFile = new File(scriptDir, "MModeTool.sh");

        String command = scriptFile.getAbsolutePath() 
            + " -u "+ageusername
            + " -p "+agepassword
            + " -h \""+agehostname+"\"" 
            + " reset";

        ProcessUtils.doCommand(command, null);
        
    }
}
