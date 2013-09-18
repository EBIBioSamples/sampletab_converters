package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import org.kohsuke.args4j.Option;

import org.mged.magetab.error.ErrorItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.utils.SubsTracking;

public class SampleTabBulk extends AbstractInfileDriver {

    @Option(name = "--hostname", aliases={"-n"}, usage = "server hostname")
    private String hostname;

    @Option(name = "--port", usage = "server port")
    private Integer port = null;

    @Option(name = "--database", aliases={"-d"}, usage = "server database")
    private String database = null;

    @Option(name = "--username", aliases={"-u"}, usage = "server username")
    private String username = null;

    @Option(name = "--password", aliases={"-p"}, usage = "server password")
    private String password  = null;
    
    @Option(name = "--force", aliases={"-f"}, usage = "overwrite targets")
    private boolean force = false;

    
    private static final String SUBSEVENT = "SampleTabBulk";

    private Corrector corrector = new Corrector();
    private DerivedFrom derivedFrom = null;
    private Accessioner accessioner = null;
    private SameAs sameAs = new SameAs();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    
    public SampleTabBulk(){
        //load defaults from file
        //will be overriden by command-line options later
        Properties properties = new Properties();
        try {
            InputStream is = SampleTabBulk.class.getResourceAsStream("/oracle.properties");
            properties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource oracle.properties", e);
        }
        this.hostname = properties.getProperty("hostname");
        this.port = new Integer(properties.getProperty("port"));
        this.database = properties.getProperty("database");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        
        try {
            accessioner = new AccessionerENA(hostname, 
                    port, database, username, password);
        } catch (ClassNotFoundException e) {
            log.error("Unable to create accessioner", e);
        } catch (SQLException e) {
            log.error("Unable to create accessioner", e);
        }
    }
    
    public SampleTabBulk(String hostname, Integer port, String database, String username, String password, boolean force){
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
        this.force = force;
    }
    
    public void process(File subdir, File scriptdir){
        Runnable t = new DoProcessFile(subdir, corrector, accessioner, sameAs, getDerivedFrom(), force);
        t.run();
    }
    
    private static class DoProcessFile implements Runnable {
        private final File sampletabpre;
        private final File sampletab;
        private final File sampletabtoload;
        
        private final Corrector corrector;
        private final SameAs sameAs;
        private final DerivedFrom derivedFrom;
        private final Accessioner accessioner;
        private final boolean force;
        
        private Logger log = LoggerFactory.getLogger(getClass());
        
        public DoProcessFile(File subdir, Corrector corrector, Accessioner accessioner, SameAs sameAs, DerivedFrom derivedFrom, boolean force){
            
            sampletabpre = new File(subdir, "sampletab.pre.txt");
            sampletab = new File(subdir, "sampletab.txt");
            sampletabtoload = new File(subdir, "sampletab.toload.txt");
            
            this.corrector = corrector;
            this.sameAs = sameAs;
            this.derivedFrom = derivedFrom;
            this.accessioner = accessioner;
            this.force = force;
        }

        public void run() {
            Date startDate = new Date();
            String accession = sampletabpre.getParentFile().getName();

            //try to register this with subs tracking
            SubsTracking.getInstance().registerEventStart(accession, SUBSEVENT, startDate, null);
            
            // accession sampletab.pre.txt to sampletab.txt
            if (force
                    || !sampletab.exists()
                    || sampletab.length() == 0
                    || sampletab.lastModified() < sampletabpre.lastModified()) {
                log.info("Processing " + sampletab);

                SampleTabSaferParser parser = new SampleTabSaferParser(new SampleTabValidator());
                
                SampleData st;
                try {
                    st = parser.parse(sampletabpre);
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletabpre, e);
                    for (ErrorItem err : e.getErrorItems()){
                        log.error(err.toString());
                    }
                    return;
                }
                
                
                try {
                    accessioner.convert(st);
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletabpre, e);
                    for (ErrorItem err : e.getErrorItems()){
                        log.error(err.toString());
                    }
                    return;
                } catch (SQLException e) {
                    log.error("Problem processing "+sampletabpre, e);
                    return;
                } catch (RuntimeException e){
                    log.error("Problem processing "+sampletabpre, e);
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
                        log.error("Unable to find derived from relationships due to error", e);
                        return;
                    }
    
                    log.info("Detecting same as...");
                    try {
                        sameAs.convert(st);
                    } catch (IOException e) {
                        log.error("Unable to find derived from relationships due to error", e);
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
                    log.error("Problem processing "+sampletabpre, e);
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
            if (force 
                    || !sampletabtoload.exists()
                    || sampletabtoload.length() == 0
                    || sampletabtoload.lastModified() < sampletab.lastModified()) {
                log.info("Processing " + sampletabtoload);

                SampleTabToLoad c;
                try {
                    c = new SampleTabToLoad(accessioner);
                    c.convert(sampletab, sampletabtoload);
                } catch (ClassNotFoundException e) {
                    log.error("Problem processing "+sampletab, e);
                    return;
                } catch (IOException e) {
                    log.error("Problem processing "+sampletab, e);
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletab, e);
                    return;
                } catch (RuntimeException e){
                    log.error("Problem processing "+sampletab, e);
                    return;
                } catch (SQLException e) {
                    log.error("Problem processing "+sampletab, e);
                    return;
                }
                log.info("Finished " + sampletabtoload);
            }
            
            Date endDate = new Date();
            
            //try to register this with subs tracking
            SubsTracking.getInstance().registerEventEnd(accession, SUBSEVENT, startDate, endDate, true);

        }
        
    }
    
    public static void main(String[] args) {
        new SampleTabBulk().doMain(args);
    }

    @Override
    protected Runnable getNewTask(File inputFile) {
        File subdir = inputFile.getAbsoluteFile().getParentFile();
        return new DoProcessFile(subdir, corrector, accessioner, sameAs, getDerivedFrom(), force);
    }

    private synchronized DerivedFrom getDerivedFrom() {
        if (derivedFrom == null) {
            derivedFrom = new DerivedFrom();
        }
        return derivedFrom;
    }
    
}
