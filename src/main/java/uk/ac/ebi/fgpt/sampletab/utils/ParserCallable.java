package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;

public class ParserCallable implements Callable<SampleData> {

    private final File inputFile;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public ParserCallable(File inputFile){
        this.inputFile = inputFile;
    }
    
    public SampleData call() throws ParseException {
        log.info("parsing "+inputFile);
        SampleTabSaferParser stParser = new SampleTabSaferParser();
        SampleData sd = null;
        sd = stParser.parse(inputFile);
        return sd;
    }
}
