package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

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

    @Option(name = "-o", aliases={"--output"}, usage = "output filename relative to input")
    private String outputFilename;

    @Option(name = "-n", aliases={"--hostname"}, usage = "server hostname")
    private String hostname;

    @Option(name = "--port", usage = "server port")
    private Integer port;

    @Option(name = "-d", aliases={"--database"}, usage = "server database")
    private String database;

    @Option(name = "-u", aliases={"--username"}, usage = "server username")
    private String username;

    @Option(name = "-p", aliases={"--password"}, usage = "server password")
    private String password;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabToLoadDriver() {
        
    }
    
    class ToLoadTask implements Runnable {
        private final File inputFile;
        private final File outputFile;

        private Logger log = LoggerFactory.getLogger(getClass());

        public ToLoadTask(File inputFile, File outputFile) {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
        }

        public void run() {
            log.info("Processing " + inputFile);

            
            SampleTabSaferParser stParser = new SampleTabSaferParser();
            SampleData sd = null;
            try {
                sd = stParser.parse(inputFile);
            } catch (ParseException e) {
                log.error("Error parsing "+inputFile, e);
                return;
            }
            if (sd == null){
                log.error("Error parsing "+inputFile);
                return;
            }

            log.info("sampletab read, preparing to convert " + inputFile);
            
            // do conversion
            SampleTabToLoad toloader;
            try {
                toloader = new SampleTabToLoad(hostname, port, database, username, password);
            } catch (ClassNotFoundException e) {
                log.error("Problem converting " + inputFile, e);
                return;
            } catch (SQLException e) {
                log.error("Problem converting " + inputFile, e);
                return;
            }
            try {
                sd = toloader.convert(sd);
            } catch (ParseException e) {
                log.error("Problem converting " + inputFile, e);
                return;
            } catch (ClassNotFoundException e) {
                log.error("Problem converting " + inputFile, e);
                return;
            } catch (SQLException e) {
                log.error("Problem converting " + inputFile, e);
                return;
            }
            
            log.info("sampletab converted, preparing to validate " + inputFile);
            
            //validate it
            LoadValidator validator = new LoadValidator();
            try {
                validator.validate(sd);
            } catch (ValidateException e) {
                log.error("Error validating "+inputFile, e);
                for (ErrorItem err : e.getErrorItems()){
                    log.error(err.reportString());
                }
                return;
            }

            log.info("sampletab validated, preparing to output to " + outputFile);
            
            // write back out
            FileWriter out = null;
            try {
                out = new FileWriter(outputFile);
            } catch (IOException e) {
                log.error("Error opening " + outputFile, e);
                return;
            }

            SampleTabWriter sampletabwriter = new SampleTabWriter(out);
            try {
                sampletabwriter.write(sd);
                sampletabwriter.close();
            } catch (IOException e) {
                log.error("Error writing " + outputFile, e);
                return;
            }
            log.debug("Processed " + inputFile);

        }
    }

    public static void main(String[] args) {
        new SampleTabToLoadDriver().doMain(args);
    }
    
    @Override
    protected void preProcess(){
        
        //load defaults
        Properties oracleProperties = new Properties();
        try {
            InputStream is = AccessionerDriver.class.getResourceAsStream("/oracle.properties");
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
    protected Runnable getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File outputFile = new File(inputFile.getParentFile(), outputFilename);
        return new ToLoadTask(inputFile, outputFile);
    }
}
