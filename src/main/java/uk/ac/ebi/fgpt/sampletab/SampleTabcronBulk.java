package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class SampleTabcronBulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename")
    private String inputFilename;

    @Option(name = "-s", aliases={"--scripts"}, usage = "script directory")
    private String scriptDirname;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleTabcronBulk() {
        
    }

    //TODO move to utils
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

    public void process(File subdir, File scriptdir){
        Runnable t = new DoProcessFile(subdir, scriptdir);
        t.run();
    }
    
    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
        }

        public void run() {
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            File sampletab = new File(subdir, "sampletab.txt");
            File sampletabtoload = new File(subdir, "sampletab.toload.txt");
            File age = new File(subdir, "age");
                        
            File target;
            
            // accession sampletab.pre.txt to sampletab.txt
            target = sampletab;
            if (!target.exists()) {
                log.info("Processing " + target);
                File script = new File(scriptdir, "SampleTabAccessioner.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                // TODO hardcoding bad
                String bashcom = script + " --input " + sampletabpre + " --output sampletab.txt"
                        + " --hostname mysql-ae-autosubs-test.ebi.ac.uk" + " --port 4340"
                        + " --database autosubs_test" + " --username admin" + " --password edsK6BV6";
                File logfile = new File(subdir, "sampletab.txt.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + target);
                    log.error("See logfile " + logfile);
                    if (target.exists()){
                        target.delete();
                        log.error("cleaning partly produced file");
                    }
                    return;
                }
            }

            // preprocess to load
            target = sampletabtoload;
            if (!sampletabtoload.exists()) {
                log.info("Processing " + sampletabtoload);
                File script = new File(scriptdir, "SampleTabToLoad.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " --input " + sampletab + " --output sampletab.toload.txt"
                        + " --hostname mysql-ae-autosubs-test.ebi.ac.uk" + " --port 4340"
                        + " --database autosubs_test" + " --username admin" + " --password edsK6BV6";
                File logfile = new File(subdir, "sampletab.toload.txt.log");
                if (!doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + sampletabtoload);
                    log.error("See logfile " + logfile);
                    if (target.exists()){
                        target.delete();
                        log.error("cleaning partly produced file");
                    }
                    return;
                }
            }

            // convert to age
            target = age;
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
                    if (target.exists()){
                        for (File todel: target.listFiles()){
                            todel.delete();
                        }
                        target.delete();
                        log.error("cleaning partly produced file");
                    }
                    return;
                }
            }
            
        }
        
    }
    
    public void run(Collection<File> files, File scriptdir) {
        
        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
        for (File subdir : files) {
            if (!subdir.isDirectory()) {
                subdir = subdir.getParentFile();
            }
            log.info("Processing "+subdir);
            Runnable t = new DoProcessFile(subdir, scriptdir);
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
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new SampleTabcronBulk().doMain(args);
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
        
        //TODO handle globs
        
        File scriptdir = new File(scriptDirname);
        
        if (!scriptdir.exists() && !scriptdir.isDirectory()) {
            log.error("Script directory missing or is not a directory");
            System.exit(1);
            return;
        }
        
        Collection<File> files = FileUtils.getMatchesGlob(inputFilename);
        
        run(files, scriptdir);
    }
}
