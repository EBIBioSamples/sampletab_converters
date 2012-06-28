package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
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
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils;

public class SampleTabBulk {

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

    @Option(name = "--no-load", usage = "Do not load into Age")
    private boolean noload = false;


    private Corrector corrector = new Corrector();
    private DerivedFrom derivedFrom = new DerivedFrom();
    private SameAs sameAs = new SameAs();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    
    public SampleTabBulk(){
        Properties mysqlProperties = new Properties();
        try {
            InputStream is = SampleTabBulk.class.getResourceAsStream("/mysql.properties");
            mysqlProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource mysql.properties");
            e.printStackTrace();
        }
        this.hostname = mysqlProperties.getProperty("hostname");
        this.port = new Integer(mysqlProperties.getProperty("port"));
        this.database = mysqlProperties.getProperty("database");
        this.username = mysqlProperties.getProperty("username");
        this.password = mysqlProperties.getProperty("password");
        
        Properties ageProperties = new Properties();
        try {
            ageProperties.load(SampleTabBulk.class.getResourceAsStream("/age.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource age.properties");
            e.printStackTrace();
        }
        this.agename = ageProperties.getProperty("hostname");
        this.ageusername = ageProperties.getProperty("username");
        this.agepassword = ageProperties.getProperty("password");
    }
    
    public SampleTabBulk(String hostname, Integer port, String database, String username, String password, String agename, String ageusername, String agepassword, Boolean noload){
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
        if (noload != null)
            this.noload = noload;
    }
    
    public void process(File subdir, File scriptdir){
        Runnable t = new DoProcessFile(subdir, scriptdir, hostname, port, database, username, password, agename, ageusername, agepassword, noload, corrector, sameAs, derivedFrom);
        t.run();
    }
    
    private static class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        private File sampletabpre;
        private File sampletab;
        private File sampletabtoload;
        private File agedir;
        private File agefile;
        private File loaddir;
        private File loadfile;

        private String hostname; 
        private int port;
        private String database;
        private String username;
        private String password;
        
        private String agename;
        private String ageusername;
        private String agepassword;
        private boolean noLoad;
        
        private Corrector corrector;
        private SameAs sameAs;
        private DerivedFrom derivedFrom;
        
        private Logger log = LoggerFactory.getLogger(getClass());
        
        public DoProcessFile(File subdir, File scriptdir, String hostname, int port, String database, String username, String password, String agename, String ageusername, String agepassword, boolean noLoad, Corrector corrector, SameAs sameAs, DerivedFrom derivedFrom){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
            
            sampletabpre = new File(subdir, "sampletab.pre.txt");
            sampletab = new File(subdir, "sampletab.txt");
            sampletabtoload = new File(subdir, "sampletab.toload.txt");
            agedir = new File(subdir, "age");
            agefile = new File(agedir, subdir.getName()+".age.txt");
            loaddir = new File(subdir, "load");
            loadfile = new File(loaddir, subdir.getName()+".SUCCESS");
            
            this.hostname = hostname;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
            
            this.agename = agename;
            this.ageusername = ageusername;
            this.agepassword = agepassword;
            this.noLoad = noLoad;
            
            this.corrector = corrector;
            this.sameAs = sameAs;
            this.derivedFrom = derivedFrom;
        }

        public void run() {
            
            // accession sampletab.pre.txt to sampletab.txt
            if (!sampletab.exists()
                    || sampletab.lastModified() < sampletabpre.lastModified()) {
                log.info("Processing " + sampletab);

                SampleTabSaferParser parser = new SampleTabSaferParser();
                SampleData st;
                try {
                    st = parser.parse(sampletabpre);
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                }
                
                
                try {
                    Accessioner accessioner = new Accessioner(hostname, 
                            port, database, username, password);
                    accessioner.convert(st);
                } catch (ClassNotFoundException e) {
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

                log.info("Applying corrections...");
                corrector.correct(st);

                //dont detect relationships for reference samples
                //these will be done manually
                if (!st.msi.submissionReferenceLayer) {
                    log.info("Detecting derived from...");
                    try {
                        derivedFrom.convert(st);
                    } catch (IOException e) {
                        log.error("Unable to find derived from relationships due to error");
                        e.printStackTrace();
                        return;
                    }
    
                    log.info("Detecting same as...");
                    try {
                        sameAs.convert(st);
                    } catch (IOException e) {
                        log.error("Unable to find derived from relationships due to error");
                        e.printStackTrace();
                        return;
                    }
                }
                
                //write it back out
                Writer writer = null;
                try {
                    writer = new FileWriter(sampletab);
                    SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
                    log.info("created SampleTabWriter");
                    sampletabwriter.write(st);
                    sampletabwriter.close();
                } catch (IOException e) {
                    log.error("Problem processing "+sampletabpre);
                    e.printStackTrace();
                    return;
                } finally {
                    if (writer != null){
                        try {
                            writer.close();
                        } catch (IOException e2) {
                            //do nothing
                        }
                    }
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
            
            if (!noLoad 
                    && (!loadfile.exists()
                            || loadfile.lastModified() < agefile.lastModified())) {
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
            Runnable t = new DoProcessFile(subdir, scriptdir, hostname, port, database, username, password, agename, ageusername, agepassword, noload, corrector, sameAs, derivedFrom);
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
        new SampleTabBulk().doMain(args);
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
