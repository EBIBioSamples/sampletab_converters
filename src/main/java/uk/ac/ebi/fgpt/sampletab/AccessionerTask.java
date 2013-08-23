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
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.SubsTracking;

public class AccessionerTask implements Runnable {
    
    private static final String SUBSEVENT = "Accessioner";
    
    private final File inputFile;
    private final File outputFile;
    private final Accessioner accessioner;
    private final Corrector corrector;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public AccessionerTask(File inputFile, File outputFile, Accessioner accessioner, Corrector corrector){
        this.inputFile = inputFile.getAbsoluteFile();
        this.outputFile = outputFile.getAbsoluteFile();
        this.accessioner = accessioner;
        this.corrector = corrector;
    }
    
    public void run() {
        Date startDate = new Date();
        String accession = inputFile.getParentFile().getName();

        //try to register this with subs tracking
        SubsTracking.getInstance().registerEventStart(accession, SUBSEVENT, startDate, null);
        
        
        SampleData st = null;
        try {
            st = accessioner.convert(inputFile);
        } catch (ParseException e) {
            log.error("ParseException converting " + inputFile, e);
            for(ErrorItem error : e.getErrorItems()){
                log.error(error.reportString());
            }
            return;
        } catch (IOException e) {
            log.error("IOException converting " + inputFile, e);
            return;
        } catch (SQLException e) {
            log.error("SQLException converting " + inputFile, e);
            return;
        }
        
        //do corrections
        if (corrector != null){
            corrector.correct(st);
        }
        
        //TODO add derived from detector here

        FileWriter out = null;
        try {
            out = new FileWriter(this.outputFile);
        } catch (IOException e) {
            log.error("Error opening " + this.outputFile, e);
            return;
        }

        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
        try {
            sampletabwriter.write(st);
        } catch (IOException e) {
            log.error("Error writing " + this.outputFile, e);
            return;
        }
        
        Date endDate = new Date();
        
        //try to register this with subs tracking
        SubsTracking.getInstance().registerEventEnd(accession, SUBSEVENT, startDate, endDate, true);
    }

}
