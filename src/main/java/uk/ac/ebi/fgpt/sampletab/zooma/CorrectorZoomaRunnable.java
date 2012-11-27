package uk.ac.ebi.fgpt.sampletab.zooma;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;

public class CorrectorZoomaRunnable implements Runnable {

    private final File inputFile;
    private final CorrectorZooma zooma = new CorrectorZooma();
    private final SampleTabSaferParser stParser = new SampleTabSaferParser();
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public CorrectorZoomaRunnable(File inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public void run() {
        SampleData sd;
        try {
            sd = stParser.parse(inputFile);
            zooma.correct(sd);
        } catch (ParseException e) {
            log.error("Unable to parse "+inputFile, e);
        }

    }

}
