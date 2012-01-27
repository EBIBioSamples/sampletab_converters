package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MageTabcronBulk {
    private Logger log = LoggerFactory.getLogger(getClass());
    // singlton instance
    private static MageTabcronBulk instance = null;

    private MageTabcronBulk() {
        // private constructor to prevent accidental multiple initialisations
    }

    public static MageTabcronBulk getInstance() {
        if (instance == null) {
            instance = new MageTabcronBulk();
        }
        return instance;
    }

    private boolean doCommand(String command) {
        return doCommand(command, null);
    }

    private boolean doCommand(String command, File logfile) {
        log.debug(command);

        ArrayList<String> bashcommand = new ArrayList<String>();
        bashcommand.add("/bin/bash");
        bashcommand.add("-c");
        bashcommand.add(command);

        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectErrorStream(true);// merge stderr to stdout
        if (logfile != null)
            pb.redirectOutput(logfile);
        pb.command(bashcommand);
        // pb.command(command.split(" "));

        Process p;
        try {
            p = pb.start();
            synchronized (p) {
                p.waitFor();
            }
            if (p.exitValue() != 0) {
                log.error("Error running " + command);
                log.error("Exit code is " + p.exitValue());
                return false;
            }
        } catch (IOException e) {
            log.error("Error running " + command);
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            log.error("Error running " + command);
            e.printStackTrace();
            return false;
        }
        return true;

    }

    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
        }

        public void run() {
            String idffilename = (subdir.getName().replace("GAE-", "E-")) + ".idf.txt";
            String sdrffilename = (subdir.getName().replace("GAE-", "E-")) + ".sdrf.txt";
            File idffile = new File(subdir, idffilename);
            File sdrffile = new File(subdir, sdrffilename);
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            File sampletab = new File(subdir, "sampletab.txt");
            File sampletabtoload = new File(subdir, "sampletab.toload.txt");
            File age = new File(subdir, "age");

            if (!idffile.exists() || !sdrffile.exists()) {
                return;
            }

            if (!sampletabpre.exists()) {
                log.info("Processing " + sampletabpre);
                // convert idf/sdrf to sampletab.pre.txt
                File script = new File(scriptdir, "MageTabToSampleTab.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " " + idffile + " " + sampletabpre;
                log.info(bashcom);
                File logfile = new File(subdir, "sampletab.pre.txt.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + sampletabpre);
                    log.error("See logfile " + logfile);
                    return;
                }
            }

            // accession sampletab.pre.txt to sampletab.txt
            if (!sampletab.exists()) {
                log.info("Processing " + sampletab);
                File script = new File(scriptdir, "SampleTabAccessioner.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                // TODO hardcoding bad
                String bashcom = script + " --input " + sampletabpre + " --output " + sampletab
                        + " --hostname mysql-ae-autosubs-test.ebi.ac.uk" + " --port 4340"
                        + " --database autosubs_test" + " --username admin" + " --password edsK6BV6";
                File logfile = new File(subdir, "sampletab.txt.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + sampletab);
                    log.error("See logfile " + logfile);
                    return;
                }
            }

            // preprocess to load
            if (!sampletabtoload.exists()) {
                log.info("Processing " + sampletabtoload);
                File script = new File(scriptdir, "SampleTabToLoad.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " --input " + sampletab + " --output " + sampletabtoload
                        + " --hostname mysql-ae-autosubs-test.ebi.ac.uk" + " --port 4340"
                        + " --database autosubs_test" + " --username admin" + " --password edsK6BV6";
                File logfile = new File(subdir, "sampletab.toload.txt.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + sampletabtoload);
                    log.error("See logfile " + logfile);
                    return;
                }
            }

            // convert to age
            if (!age.exists()) {
                log.info("Processing " + age);
                File script = new File(scriptdir, "SampleTab-to-AGETAB.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " -o " + age + " " + sampletabtoload;
                File logfile = new File(subdir, "age.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + age);
                    log.error("See logfile " + logfile);
                    return;
                }
            }
            
        }
        
    }
    
    public void run(File dir, File scriptdir) {
        dir = dir.getAbsoluteFile();
        scriptdir = scriptdir.getAbsoluteFile();

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads*2);
        
        for (File subdir : dir.listFiles()) {
            if (subdir.isDirectory()) {
                DoProcessFile todo = new DoProcessFile(subdir, scriptdir);
                todo.run();
                
                //pool.execute( new DoProcessFile(subdir, scriptdir));
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
        if (args.length < 1) {
            System.err.println("Must provide the following paramters:");
            System.err.println("  ArrayExpress local directory");
            System.err.println("  script directory");
            System.exit(1);
            return;
        }

        File outdir = new File(args[0]);

        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Target is not a directory");
            System.exit(1);
            return;
        }

        if (!outdir.exists())
            outdir.mkdirs();

        File scriptdir = new File(args[1]);

        getInstance().run(outdir, scriptdir);
    }
}
