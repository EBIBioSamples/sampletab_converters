package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.subs.Event;
import uk.ac.ebi.fgpt.sampletab.subs.TrackingManager;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;

public class ENASRAUpdateRunnable implements Runnable {
    
    private static final String SUBSEVENT = "Source Update";

    private final File outsubdir;
    private final String keyId;
    private final Collection<String> sampleIds;
    private final boolean conan;
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public ENASRAUpdateRunnable(File outsubdir, String key, Collection<String> samples, boolean conan) {
        this.outsubdir = outsubdir;
        this.keyId = key;
        this.sampleIds = samples;
        this.conan = conan;
    }

    @Override
    public void run() {
        String accession = outsubdir.getName();

        //try to register this with subs tracking
        Event event = TrackingManager.getInstance().registerEventStart(accession, SUBSEVENT);
        
        ENASRAXMLToSampleTab converter = new ENASRAXMLToSampleTab();
        SampleData sd = null;
        try {
            sd = converter.convert(new File(outsubdir, ""+keyId+".xml"), sampleIds);
        } catch (ParseException e) {
            log.error("Problem processing "+keyId, e);
        } catch (IOException e) {
            log.error("Problem processing "+keyId, e);
        } catch (DocumentException e) {
            log.error("Problem processing "+keyId, e);
        }
        
        if (sd != null) {
            File sampletabPre = new File(outsubdir, "sampletab.pre.txt");
            SampleTabWriter sampletabwriter = null;
            try {
                sampletabwriter = new SampleTabWriter(new BufferedWriter(new FileWriter(sampletabPre)));
                sampletabwriter.write(sd);
            } catch (IOException e) {
                log.error("Unable to write to "+sampletabPre, e);
            } finally {
                if (sampletabwriter != null){
                    try {
                        sampletabwriter.close();
                    } catch (IOException e) {
                        //do nothing
                    }
                }
            }
        }
        
        //trigger conan if appropriate
        if (conan) {
            try {
                ConanUtils.submit(sd.msi.submissionIdentifier, "BioSamples (other)");
            } catch (IOException e) {
                log.error("Problem submitting to conan "+sd.msi.submissionIdentifier, e);
            }
        }
                
        //try to register this with subs tracking
        TrackingManager.getInstance().registerEventEnd(event);

    }

}
