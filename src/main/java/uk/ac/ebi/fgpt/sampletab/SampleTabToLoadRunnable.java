package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.Callable;

import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.subs.Event;
import uk.ac.ebi.fgpt.sampletab.subs.TrackingManager;

public class SampleTabToLoadRunnable implements Callable<Void> {
    
    private static final String SUBSEVENT = "ToLoad";
    
    private final File inputFile;
    private final File outputFile;
    
    private final String host;
    private final int port;
    private final String database;
    private final String dbusername;
    private String dbpassword;
    
    private final boolean noGroup;

    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleTabToLoadRunnable(File inputFile, File outputFile, 
            String host, int port, String database, String dbusername, String dbpassword, 
            boolean noGroup) {
        
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        
        this.host = host;
        this.port = port;
        this.database = database;
        this.dbusername = dbusername;
        this.dbpassword = dbpassword;
        
        this.noGroup = noGroup;
    }

    @Override
    public Void call() throws Exception {
        log.info("Processing " + inputFile);
        String accession = inputFile.getParentFile().getName();

        //try to register this with subs tracking
        Event event = TrackingManager.getInstance().registerEventStart(accession, SUBSEVENT);
        
        try {
            doWork();
        } finally {
            //try to register this with subs tracking
            TrackingManager.getInstance().registerEventEnd(event);
        }
        return null;
    }
    
    private void doWork() throws Exception {
        
        SampleTabSaferParser stParser = new SampleTabSaferParser();
        SampleData sd = null;
        sd = stParser.parse(inputFile);
        if (sd == null){
            log.error("Error parsing "+inputFile);
            return;
        }

        log.info("sampletab read, preparing to convert " + inputFile);
        
        // do conversion
        SampleTabToLoad toloader;
        toloader = new SampleTabToLoad(host, port, database, dbusername, dbpassword);
        toloader.setInGroup(!noGroup);
        sd = toloader.convert(sd);
        
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
            throw e;
        }

        log.info("sampletab validated, preparing to output to " + outputFile);
        
        // write back out
        FileWriter out = null;
        try {
            out = new FileWriter(outputFile);

            SampleTabWriter sampletabwriter = new SampleTabWriter(out);
            sampletabwriter.write(sd);
            sampletabwriter.close();
        } finally {
            if (out != null) {
                out.close();
            }
        }
        log.debug("Processed " + inputFile);
    }

}
