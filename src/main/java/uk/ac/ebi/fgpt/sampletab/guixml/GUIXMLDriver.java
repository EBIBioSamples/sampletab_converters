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
    
    @Option(name = "--tempdir", aliases = { "-d" }, usage = "temporary directory")
    private File tempdir = null;
    
    private GUIXMLOutputer outputer;
    private File temporaryFile = null;
    
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
        try {
            temporaryFile = File.createTempFile("gui", "xml", tempdir);
            temporaryFile.deleteOnExit();
        } catch (IOException e) {
            log.error("Problem creating temporary directory");
            throw new RuntimeException(e);
        }
        
        temporaryFile = temporaryFile.getAbsoluteFile();
        if (!temporaryFile.getParentFile().exists() && !temporaryFile.getParentFile().mkdirs()){
            log.error("Unable to make directories for "+temporaryFile);
            throw new RuntimeException();
        }
        
        outputer = new GUIXMLOutputer(temporaryFile);
        try {
            outputer.start();
        } catch (SaxonApiException e) {
            log.error("Error setting up output to "+temporaryFile, e);
            throw new RuntimeException(e);
        } catch (XMLStreamException e) {
            log.error("Error setting up output to "+temporaryFile, e);
            throw new RuntimeException(e);
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

        File outputFile = new File(outputFilename);
        try {
            FileUtils.move(temporaryFile, outputFile);
        } catch (IOException e) {
            log.error("Error moving output to final destination", e);
            return;
        }
    }

}
