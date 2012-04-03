package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
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

    @Option(name = "-n", aliases={"--hostname"}, usage = "MySQL server hostname")
    private String hostname;

    @Option(name = "-t", aliases={"--port"}, usage = "MySQL server port")
    private Integer port = null;

    @Option(name = "-d", aliases={"--database"}, usage = "MySQL server database")
    private String database = null;

    @Option(name = "-u", aliases={"--username"}, usage = "MySQL server username")
    private String username = null;

    @Option(name = "-p", aliases={"--password"}, usage = "MySQL server password")
    private String password  = null;

    @Option(name = "--agename", usage = "Age server hostname")
    private String agename = null;

    @Option(name = "--ageusername", usage = "Age server username")
    private String ageusername = null;

    @Option(name = "--agepassword", usage = "Age server password")
    private String agepassword = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    
    public SampleTabcronBulk(){
        Properties mysqlProperties = new Properties();
        try {
            InputStream is = SampleTabcronBulk.class.getResourceAsStream("/mysql.properties");
            mysqlProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource mysql.properties");
            e.printStackTrace();
        }
        Properties ageProperties = new Properties();
        try {
            ageProperties.load(SampleTabcronBulk.class.getResourceAsStream("/age.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource age.properties");
            e.printStackTrace();
        }
        this.hostname = mysqlProperties.getProperty("hostname");
        this.port = new Integer(mysqlProperties.getProperty("port"));
        this.database = mysqlProperties.getProperty("database");
        this.username = mysqlProperties.getProperty("username");
        this.password = mysqlProperties.getProperty("password");
        this.agename = ageProperties.getProperty("hostname");
        this.ageusername = ageProperties.getProperty("username");
        this.agepassword = ageProperties.getProperty("password");
    }
    
    public SampleTabcronBulk(String hostname, Integer port, String database, String username, String password, String agename, String ageusername, String agepassword){
        this();
        if (hostname != null)
            this.hostname = hostname;
        if (port != null)
            this.port = port;
        if (database != null)
            this.database = database;
        if (username != null)
            this.username = username;
        if (password != null)
            this.password = password;
        if (agename != null)
            this.agename = agename;
        if (ageusername != null)
            this.ageusername = ageusername;
        if (agepassword != null)
            this.agepassword = agepassword;
    }
    
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
        private File agedir;
        private File agefile;
        private File loaddir;
        private File loadfile;
        
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
            
            sampletabpre = new File(subdir, "sampletab.pre.txt");
            sampletab = new File(subdir, "sampletab.txt");
            sampletabtoload = new File(subdir, "sampletab.toload.txt");
            agedir = new File(subdir, "age");
            agefile = new File(agedir, subdir.getName()+".age.txt");
            loaddir = new File(subdir, "load");
            loadfile = new File(loaddir, subdir.getName()+".SUCCESS");
        }

        public void run() {
            
            // accession sampletab.pre.txt to sampletab.txt
            if (!sampletab.exists()
                    || sampletab.lastModified() < sampletabpre.lastModified()) {
                log.info("Processing " + sampletab);
                
                try {
                    SampleTabAccessioner c = new SampleTabAccessioner(hostname, 
                            port, database, username, password);
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
            if (!sampletabtoload.exists()
                    || sampletabtoload.lastModified() < sampletab.lastModified()) {
                log.info("Processing " + sampletabtoload);

                SampleTabToLoad c;
                try {
                    c = new SampleTabToLoad(hostname, 
                            port, database, username, password);
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
                log.info("Finished " + sampletabtoload);
            }

            // convert to age
            if (!agefile.exists()
                    || agefile.lastModified() < sampletabtoload.lastModified()) {
                log.info("Processing " + agefile);
                File script = new File(scriptdir, "SampleTab-to-AGETAB.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " -o " + agedir + " " + sampletabtoload;
                
                SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                File logfile = new File(subdir, "age_"+simpledateformat.format(new Date())+".log");
                
                if (!ProcessUtils.doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + agefile);
                    log.error("See logfile " + logfile);
                    if (agedir.exists()){
                        for (File todel: agedir.listFiles()){
                            todel.delete();
                        }
                        agedir.delete();
                        log.error("cleaning partly produced file");
                    }
                    return;
                }
                log.info("Finished " + agefile);
            }
            
            if (!loadfile.exists()
                    || loadfile.lastModified() < agefile.lastModified()) {
                log.info("Processing " + loadfile);
                File script = new File(scriptdir, "AgeTab-Loader.sh");
                if (!script.exists()) {
                    log.error("Unable to find " + script);
                    return;
                }
                String bashcom = script + " -s -m -i -e -o \""+loaddir+"\" -h \""+agename+"\" -u "+ageusername+" -p "+agepassword+" \""+agedir+"\"";
                log.info(bashcom);
                SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                File logfile = new File(subdir, "load_"+simpledateformat.format(new Date())+".log");
                
                if (!ProcessUtils.doCommand(bashcom, logfile)) {
                    log.error("Problem producing " + loadfile);
                    log.error("See logfile " + logfile);
                    return;
                }
                log.info("Finished " + loadfile);
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
