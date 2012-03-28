package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
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

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;

public class SampleTabcronBulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input directory or glob")
    private String inputFilename;

    @Option(name = "-s", aliases={"--scripts"}, usage = "script directory")
    private String scriptDirname;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "-n", aliases={"--hostname"}, usage = "server hostname")
    private String hostname;

    @Option(name = "-t", aliases={"--port"}, usage = "server port")
    private int port = 3306;

    @Option(name = "-d", aliases={"--database"}, usage = "server database")
    private String database;

    @Option(name = "-u", aliases={"--username"}, usage = "server username")
    private String username;

    @Option(name = "-p", aliases={"--password"}, usage = "server password")
    private String password;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public void process(File subdir, File scriptdir){
        Runnable t = new DoProcessFile(subdir, scriptdir);
        t.run();
    }
    
    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        private File sampletabpre;
        private File sampletab;
        private File sampletabtoload;
        private File age;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
            
            sampletabpre = new File(subdir, "sampletab.pre.txt");
            sampletab = new File(subdir, "sampletab.txt");
            sampletabtoload = new File(subdir, "sampletab.toload.txt");
            age = new File(subdir, "age");
        }

        public void run() {

            // TODO hardcoding bad
            String accessionServer = "mysql-ae-autosubs-test.ebi.ac.uk";
            int accessionPort = 4340;
            String accessionDB = "autosubs_test";
            String accessionUser = "admin";
            String accessionPassword = "edsK6BV6";
            
            
            File target;
            
            // accession sampletab.pre.txt to sampletab.txt
            target = sampletab;
            if (!target.exists()
                    || sampletab.lastModified() < sampletabpre.lastModified()) {
                log.info("Processing " + target);
                
                try {
                    SampleTabAccessioner c = new SampleTabAccessioner(accessionServer, 
                            accessionPort, accessionDB, accessionUser, accessionPassword);
                    c.convert(sampletabpre, sampletab);
                } catch (ClassNotFoundException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                } catch (IOException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                } catch (SQLException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                } catch (RuntimeException e){
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                }
            }

            // preprocess to load
            target = sampletabtoload;
            if (!target.exists()
                    || sampletabtoload.lastModified() < sampletabtoload.lastModified()) {
                log.info("Processing " + target);

                SampleTabToLoad c;
                try {
                    c = new SampleTabToLoad(accessionServer, 
                            accessionPort, accessionDB, accessionUser, accessionPassword);
                    c.convert(sampletab, sampletabtoload);
                } catch (ClassNotFoundException e) {
                    log.error("Problem processing "+sampletab);
                    e.printStackTrace();
                    return;
                } catch (IOException e) {
                    log.error("Problem processing "+sampletab);
                    e.printStackTrace();
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletab);
                    e.printStackTrace();
                    return;
                } catch (RuntimeException e){
                    log.error("Problem processing "+sampletab);
                    e.printStackTrace();
                    return;
                } catch (SQLException e) {
                    log.error("Problem processing "+sampletab);
                    e.printStackTrace();
                    return;
                }
                log.info("Finished " + target);
            }

            // convert to age
            target = age;
            if (!target.exists()) {
                //TODO check modification time
                log.info("Processing " + target);
                File script = new File(scriptdir, "SampleTab-to-AGETAB.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " -o " + age + " " + sampletabtoload;
                File logfile = new File(subdir, "age.log");
                if (!ProcessUtils.doCommand(bashcom, logfile)) {
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
                log.info("Finished " + target);
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
