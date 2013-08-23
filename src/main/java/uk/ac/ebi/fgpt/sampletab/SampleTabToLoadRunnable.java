package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.SubsTracking;

public class SampleTabToLoadRunnable implements Runnable {
    
    private static final String SUBSEVENT = "ToLoad";
    
    private final File inputFile;
    private final File outputFile;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private String password;

    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleTabToLoadRunnable(File inputFile, File outputFile, String host, int port, String database, String username, String password) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }
    

    public void run() {
        Date startDate = new Date();
        String accession = inputFile.getParentFile().getName();

        //try to register this with subs tracking
        SubsTracking.getInstance().registerEventStart(accession, SUBSEVENT, startDate, null);
        
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
            toloader = new SampleTabToLoad(host, port, database, username, password);
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

        Date endDate = new Date();
        
        //try to register this with subs tracking
        SubsTracking.getInstance().registerEventEnd(accession, SUBSEVENT, startDate, endDate, true);
    }

}
