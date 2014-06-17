package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for sampletab.pre.txt -> sampletab.txt -> sampletab.toload.txt
 * 
 * 
 * @author faulcon
 *
 */
public class SampleTabBulkDriver extends AbstractInfileDriver<SampleTabBulkRunnable> {

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
    
    @Option(name = "--no-load", aliases={"-l"}, usage = "skip creating sampletab.toload.txt")
    private boolean noload = false;
    
    @Option(name = "--no-group", aliases={"-g"}, usage = "skip creating sampletab.toload.txt")
    private boolean nogroup = false;
    
    @Option(name = "--root", usage = "root directory for sampletab files")
    private File rootDir = null;

    private Corrector corrector = new Corrector();
    private CorrectorAddAttr correctorAddAttr = null;
    private DerivedFrom derivedFrom = null;
    private Accessioner accessioner = null;
    private SameAs sameAs = new SameAs();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    
    public SampleTabBulkDriver() {
        //load defaults from file
        //will be overridden by command-line options later
        Properties properties = new Properties();
        try {
            InputStream is = SampleTabBulkDriver.class.getResourceAsStream("/oracle.properties");
            properties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource oracle.properties", e);
        }
        
        this.hostname = properties.getProperty("hostname");
        this.port = new Integer(properties.getProperty("port"));
        this.database = properties.getProperty("database");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        
        accessioner = new AccessionerENA(hostname, 
                port, database, username, password);
        correctorAddAttr = new CorrectorAddAttr(hostname, 
                port, database, username, password);
        
        properties = new Properties();
        try {
            InputStream is = this.getClass().getResourceAsStream("/sampletabconverters.properties");
            properties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource sampletabconverters.properties", e);
            return;
        }
        rootDir = new File(properties.getProperty("biosamples.sampletab.path"));
    }
    
    public SampleTabBulkDriver(String hostname, Integer port, String database, String username, String password, boolean force) {
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
        
    public static void main(String[] args) {
        new SampleTabBulkDriver().doMain(args);
    }

    @Override
    protected SampleTabBulkRunnable getNewTask(File inputFile) {
        File subdir = inputFile.getAbsoluteFile().getParentFile();
        return new SampleTabBulkRunnable(subdir, corrector, correctorAddAttr, 
            accessioner, sameAs, getDerivedFrom(), force, noload, nogroup);
    }
    
    private synchronized DerivedFrom getDerivedFrom() {
        if (derivedFrom == null) {
            derivedFrom = new DerivedFrom(rootDir);
        }
        return derivedFrom;
    }
    
    @Override
    protected void postProcess() {
        log.info("closing accessioner");
        accessioner.close();
    }
}
