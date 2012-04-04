package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;

public class DGVaXMLcronBulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    //TODO make required
    @Option(name = "-i", aliases={"--input"}, usage = "input filename")
    private String inputFilename;

    //TODO make required
    @Option(name = "-s", aliases={"--scripts"}, usage = "script directory")
    private String scriptDirname;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "-n", aliases={"--hostname"}, usage = "MySQL server hostname")
    private String hostname = null;

    @Option(name = "-t", aliases={"--port"}, usage = "MySQL server port")
    private Integer port = null;

    @Option(name = "-d", aliases={"--database"}, usage = "MySQL server database")
    private String database = null;

    @Option(name = "-u", aliases={"--username"}, usage = "MySQL server username")
    private String username = null;

    @Option(name = "-p", aliases={"--password"}, usage = "MySQL server password")
    private String password = null;

    @Option(name = "--agename", usage = "Age server hostname")
    private String agename = null;

    @Option(name = "--ageusername", usage = "Age server username")
    private String ageusername = null;

    @Option(name = "--agepassword", usage = "Age server password")
    private String agepassword = null;

    @Option(name = "--no-load", usage = "Do not load into Age")
    private boolean noload = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());


    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
        }

        public void run() {
            String studyFilename = "raw.xml";
            File xmlFile = new File(subdir, studyFilename);
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            
            
            if (!xmlFile.exists()) {
                return;
            }
            
            File target;
            
            target = sampletabpre;
            if (!target.exists()
                    || target.lastModified() < xmlFile.lastModified()) {
                log.info("Processing " + target);
                // convert study.xml to sampletab.pre.txt

                try {
                    new DGVaXMLToSampleTab().convert(xmlFile, sampletabpre);
                } catch (IOException e) {
                    log.error("Problem processing "+xmlFile);
                    e.printStackTrace();
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+xmlFile);
                    e.printStackTrace();
                    return;
                } catch (RuntimeException e) {
                    log.error("Problem processing "+xmlFile);
                    e.printStackTrace();
                    return;
                }
                
            }

            //now run the other SampleTab processes
            new SampleTabcronBulk(hostname, port, database, username, password, agename, ageusername, agepassword, noload).process(subdir, scriptdir);
        }
        
    }
    
    public void run(File dir, File scriptdir) {
        dir = dir.getAbsoluteFile();
        scriptdir = scriptdir.getAbsoluteFile();

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
        File[] subdirs = dir.listFiles();
        Arrays.sort(subdirs);
        for (File subdir : subdirs) {
            if (subdir.isDirectory()) {
                Runnable t = new DoProcessFile(subdir, scriptdir);
                if (threaded) {
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
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new DGVaXMLcronBulk().doMain(args);
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
        
        File outdir = new File(inputFilename);
        
        if (outdir.exists() && !outdir.isDirectory()) {
            log.error("Target is not a directory");
            System.exit(1);
            return;
        }

        if (!outdir.exists())
            outdir.mkdirs();

        File scriptdir = new File(scriptDirname);
        
        if (!outdir.exists() && !outdir.isDirectory()) {
            log.error("Script directory missing or is not a directory");
            System.exit(1);
            return;
        }
        
        run(outdir, scriptdir);
    }
}
