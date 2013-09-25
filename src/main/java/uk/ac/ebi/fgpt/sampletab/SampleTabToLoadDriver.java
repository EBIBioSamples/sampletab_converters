package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.Option;
import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class SampleTabToLoadDriver extends AbstractInfileDriver {

    @Option(name = "--output", aliases={"-o"}, usage = "output filename relative to input")
    private String outputFilename;

    @Option(name = "--hostname", aliases={"-n"}, usage = "server hostname")
    private String hostname;

    @Option(name = "--port", usage = "server port")
    private Integer port;

    @Option(name = "--database", aliases={"-d"}, usage = "server database")
    private String database;

    @Option(name = "--username", aliases={"-u"}, usage = "server username")
    private String username;

    @Option(name = "--password", aliases={"-p"}, usage = "server password")
    private String password;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabToLoadDriver() {
        
    }
    

    public static void main(String[] args) {
        new SampleTabToLoadDriver().doMain(args);
    }
    
    @Override
    protected void preProcess(){
        
        //load defaults
        Properties oracleProperties = new Properties();
        try {
            InputStream is = getClass().getResourceAsStream("/oracle.properties");
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
    }

    @Override
    protected Callable<Void> getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File outputFile = new File(inputFile.getParentFile(), outputFilename);
        return new SampleTabToLoadRunnable(inputFile, outputFile, hostname, port, database, username, password);
    }
}
