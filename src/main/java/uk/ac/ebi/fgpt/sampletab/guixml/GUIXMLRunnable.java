package uk.ac.ebi.fgpt.sampletab.guixml;

import java.io.File;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;

public class GUIXMLRunnable implements Runnable {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private final GUIXMLOutputer outputer;
    private final File inputFile;
    
    public GUIXMLRunnable(GUIXMLOutputer outputer, File inputFile){
        this.outputer = outputer;
        this.inputFile = inputFile;
    }

    @Override
    public void run() {
        SampleTabSaferParser stParser = new SampleTabSaferParser();
        SampleData sd = null;
        try {
            sd = stParser.parse(inputFile);
        } catch (ParseException e) {
            log.error("Error parsing "+inputFile, e);
        }
        if (sd != null){
            try {
                outputer.process(sd);
            } catch (XMLStreamException e) {
                log.error("Error outputing "+inputFile, e);
            }
        }
    }
}
