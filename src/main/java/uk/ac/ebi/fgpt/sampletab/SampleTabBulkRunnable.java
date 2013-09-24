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
import uk.ac.ebi.fgpt.sampletab.subs.Event;
import uk.ac.ebi.fgpt.sampletab.subs.TrackingManager;

public class SampleTabBulkRunnable implements Runnable {
    private final File sampletabpre;
    private final File sampletab;
    private final File sampletabtoload;
    
    private final Corrector corrector;
    private final SameAs sameAs;
    private final DerivedFrom derivedFrom;
    private final Accessioner accessioner;
    private final boolean force;
    private final boolean noload;

    private static final String SUBSEVENT = "SampleTabBulk";
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabBulkRunnable(File subdir, Corrector corrector, Accessioner accessioner, SameAs sameAs, DerivedFrom derivedFrom, boolean force, boolean noload) {
        
        sampletabpre = new File(subdir, "sampletab.pre.txt");
        sampletab = new File(subdir, "sampletab.txt");
        sampletabtoload = new File(subdir, "sampletab.toload.txt");
        
        this.corrector = corrector;
        this.sameAs = sameAs;
        this.derivedFrom = derivedFrom;
        this.accessioner = accessioner;
        this.force = force;
        this.noload = noload;
    }

    public void run() {
        String accession = sampletabpre.getParentFile().getName();

        //try to register this with subs tracking
        Event event = TrackingManager.getInstance().registerEventStart(accession, SUBSEVENT);
        
        try {
            doWork();
        } finally {       
            //try to register this with subs tracking
            TrackingManager.getInstance().registerEventEnd(event);
        }

    }
    

    private void doWork() {

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
        if (!noload) {
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
        }
    }
           
}
