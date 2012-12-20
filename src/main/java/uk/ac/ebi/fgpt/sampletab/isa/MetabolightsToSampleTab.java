package uk.ac.ebi.fgpt.sampletab.isa;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
/*
import org.isatools.isatab.ISATABLoader;
import org.isatools.isatab.ISATABValidator;
import org.isatools.isatab.gui_invokers.GUIInvokerResult;
import org.isatools.tablib.schema.FormatSetInstance;
import org.isatools.tablib.utils.BIIObjectStore;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.validator.Validator;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

public class MetabolightsToSampleTab {
/*
    // logging
    public Logger log = LoggerFactory.getLogger(getClass());


    public SampleData convert(BIIObjectStore bii)
            throws ParseException {
        SampleData st = new SampleData();
        
        //TODO FINISH ME
        
        throw new RuntimeException("Not implemented yet");
    }
    
    public SampleData convert(String inputFilename) throws IOException, ParseException {
        return convert(new File(inputFilename));
    }
    
    public SampleData convert(File inputFile) throws IOException, ParseException {

        ISATABLoader isatabLoader = new ISATABLoader(inputFile.getAbsolutePath());
        FormatSetInstance isatabInstance = isatabLoader.load();

        ISATABValidator validator = new ISATABValidator(isatabInstance);
        if (GUIInvokerResult.WARNING == validator.validate()) {
            log.warn("WARNING: ISATAB validation reports problems, see log messages");
        }

        return convert(validator.getStore());
    }
    
    public void convert(BIIObjectStore bii, Writer writer)
            throws IOException, ParseException {
        SampleData st = convert(bii);
        Validator<SampleData> validator = new SampleTabValidator();
        validator.validate(st);
        
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.debug("created SampleTabWriter");
        sampletabwriter.write(st);
        sampletabwriter.close();
    }

    public void convert(File inputFile, Writer writer) throws IOException,
            ParseException {

        SampleData st = convert(inputFile);

        Validator<SampleData> validator = new SampleTabValidator();
        validator.validate(st);
        
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.debug("created SampleTabWriter");
        sampletabwriter.write(st);
        sampletabwriter.close();
    }

    public void convert(File inputFile, String stfilename) throws IOException,
            ParseException {
        convert(inputFile, new File(stfilename));
    }

    public void convert(File inputFile, File stfile) throws IOException,
            ParseException {
        convert(inputFile, new FileWriter(stfile));
    }

    public void convert(String inputFilename, Writer writer) throws IOException,
            ParseException {
        convert(new File(inputFilename), writer);
    }

    public void convert(String inputFilename, File outputFile) throws IOException,
            ParseException, java.text.ParseException {
        convert(inputFilename, new BufferedWriter(new FileWriter(outputFile)));
    }

    public void convert(String inputFilename, String outputFilename)
            throws IOException, ParseException, java.text.ParseException {
        convert(inputFilename, new File(outputFilename));
    }
    */
}
