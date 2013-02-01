package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class AccessionerDriver {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename or glob")
    private String inputFilename;

    @Option(name = "-o", aliases={"--output"}, usage = "output filename")
    private String outputFilename;

    @Option(name = "-n", aliases={"--hostname"}, usage = "server hostname")
    private String hostname;

    @Option(name = "-t", aliases={"--port"}, usage = "server port")
    private Integer port;

    @Option(name = "-d", aliases={"--database"}, usage = "server database")
    private String database;

    @Option(name = "-u", aliases={"--username"}, usage = "server username")
    private String username;

    @Option(name = "-p", aliases={"--password"}, usage = "server password")
    private String password;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    private Accessioner accessioner = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new AccessionerDriver().doMain(args);
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
            System.exit(1);
            return;
        }
        
        
        //load defaults
        Properties oracleProperties = new Properties();
        try {
            InputStream is = AccessionerDriver.class.getResourceAsStream("/oracle.properties");
            oracleProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource oracle.properties", e);
        }
        if (hostname == null){
            hostname = oracleProperties.getProperty("hostname");
        }
        if (port == null){
            port = new Integer(oracleProperties.getProperty("port"));
        }
        if (database == null){
            database = oracleProperties.getProperty("database");
        }
        if (username == null){
            username = oracleProperties.getProperty("username");
        }
        if (password == null){
            password = oracleProperties.getProperty("password");
        }

        //create the accessioner
        try {
            accessioner = new AccessionerENA(hostname, port, database, username, password);
        } catch (ClassNotFoundException e) {
            log.error("Unable to find interface class ", e);
            System.exit(2);
            return;
        } catch (SQLException e) {
            log.error("Error in SQL", e);
            System.exit(2);
            return;
        }
        
        log.debug("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files from "+inputFilename);
        Collections.sort(inputFiles);
        
        if (inputFiles.size() == 0){
            log.error("No input files found");
            System.exit(3);
            return;
        }
        
        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        Corrector c = new Corrector();

        for (File inputFile : inputFiles) {
            // System.out.println("Checking "+inputFile);
            File outputFile = new File(inputFile.getParentFile(), outputFilename);
            if (!outputFile.exists() 
                    || outputFile.lastModified() < inputFile.lastModified()) {
                Runnable t = new AccessionTask(inputFile, outputFile, accessioner, c);
                if (threaded){
                    pool.execute(t);
                } else {
                    t.run();
                }
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
                System.exit(4);
            }
        }
        log.info("Finished processing");
        
        //check that all the required output files were created
        int exitcode = 0;
        for (File inputFile : inputFiles) {
            // System.out.println("Checking "+inputFile);
            File outputFile = new File(inputFile.getParentFile(), outputFilename);
            if (!outputFile.exists() 
                    || outputFile.lastModified() < inputFile.lastModified()) {
                log.error("Unable to find output file "+outputFile);
                exitcode = 5;
            }
        }
        System.exit(exitcode);
    }
}
