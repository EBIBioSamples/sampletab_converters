package uk.ac.ebi.fgpt.sampletab.guixml;

import java.io.File;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import net.sf.saxon.s9api.SaxonApiException;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class GUIXMLDriver extends AbstractInfileDriver<GUIXMLRunnable> {

    @Option(name = "--output", aliases = { "-o" }, usage = "output filename")
    private String outputFilename = "gui.xml";
    
    private GUIXMLOutputer outputer;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public static void main(String[] args) {
        new GUIXMLDriver().doMain(args);
    }
    
    
    @Override
    protected GUIXMLRunnable getNewTask(File inputFile) {
        return new GUIXMLRunnable(outputer, inputFile);
    }
    
    @Override
    protected void preProcess() {
        
        File outputFile = new File(outputFilename+".tmp");
        outputFile = outputFile.getAbsoluteFile();
        if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()){
            log.error("Unable to make directories for "+outputFile);
            return;
        }
        
        outputer = new GUIXMLOutputer(outputFile);
        try {
            outputer.start();
        } catch (SaxonApiException e) {
            log.error("Error setting up output to "+outputFile, e);
            return;
        } catch (XMLStreamException e) {
            log.error("Error setting up output to "+outputFile, e);
            return;
        }
    }
    
    @Override
    protected void postProcess(){
        log.info("postProcess-ing");
        try {
            outputer.end();
        } catch (XMLStreamException e) {
            log.error("Error tidying up output", e);
            return;
        } catch (IOException e) {
            log.error("Error tidying up output", e);
            return;
        }

        File outputFileTmp = new File(outputFilename+".tmp");
        File outputFile = new File(outputFilename);
        try {
            FileUtils.move(outputFileTmp, outputFile);
        } catch (IOException e) {
            log.error("Error moving output to final destination", e);
            return;
        }
    }

}
