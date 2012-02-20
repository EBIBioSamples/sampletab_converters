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

import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;

public class IMSRTabcronBulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename")
    private String inputFilename;

    @Option(name = "-s", aliases={"--scripts"}, usage = "script directory")
    private String scriptDirname;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
        }

        public void run() {
            String tabFilename = "raw.tab.txt";
            File tabFile = new File(subdir, tabFilename);
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            
            
            if (!tabFile.exists()) {
                return;
            }
            
            File target;
            
            target = sampletabpre;
            if (!target.exists()
                    || target.lastModified() < tabFile.lastModified()) {
                log.info("Processing " + target);
                
                // convert raw.tab.txt to sampletab.pre.txt
                //TODO do this directly in this process
                File script = new File(scriptdir, "IMSRTabToSampleTab.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " " + tabFile + " " + sampletabpre;
                log.info(bashcom);
                File logfile = new File(subdir, "sampletab.pre.txt.log");
                if (!ProcessUtils.doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + target);
                    log.error("See logfile " + logfile);
                    if (target.exists()){
                        target.delete();
                        log.error("cleaning partly produced file");
                    }
                    return;
                }
            }

            new SampleTabcronBulk().process(subdir, scriptdir);
        }
        
    }
    
    public void run(File dir, File scriptdir) {

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
        new IMSRTabcronBulk().doMain(args);
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
