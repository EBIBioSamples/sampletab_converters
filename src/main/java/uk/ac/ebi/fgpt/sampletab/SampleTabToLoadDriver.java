package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import org.kohsuke.args4j.Option;
import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.LoadValidator;

public class SampleTabToLoadDriver extends AbstractInfileDriver {

    @Option(name = "-o", aliases={"--output"}, usage = "output filename relative to input")
    private String outputFilename;

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

    private Accessioner accessioner;

    private Logger log = LoggerFactory.getLogger(getClass());

    
    
    public SampleTabToLoadDriver() {
        // do nothing
    }

    public SampleTabToLoadDriver(String host, int port, String database, String username, String password)
            throws ClassNotFoundException {
        this();
        // Setup the connection with the DB
        this.username = username;
        this.password = password;
        this.hostname = host;
        this.port = port;
        this.database = database;
    }

    public SampleTabToLoadDriver(Accessioner accessioner)
            throws ClassNotFoundException {
        this();
        this.accessioner = accessioner;
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

            log.info("sampletab read, preparing to convert");
            
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
            
            log.info("sampletab converted, preparing to validate");
            
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

            log.info("sampletab validated, preparing to output");
            
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
    

    protected void preProcess(){
        
        if (accessioner == null){
            try {
                accessioner = new Accessioner(hostname, 
                        port, database, username, password);
            } catch (ClassNotFoundException e) {
                log.error("Unable to create accessioner", e);
            } catch (SQLException e) {
                log.error("Unable to create accessioner", e);
            }
        }
        
    }

    @Override
    protected Runnable getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File outputFile = new File(inputFile.getParentFile(), outputFilename);
        return new ToLoadTask(inputFile, outputFile);
    }
}
