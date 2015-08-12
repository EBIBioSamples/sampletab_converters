package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTabToLoadDriver extends AbstractInfileDriver {

    @Option(name = "--output", aliases={"-o"}, usage = "output filename relative to input")
    private String outputFilename;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabToLoadDriver() {
        
    }
    

    public static void main(String[] args) {
        new SampleTabToLoadDriver().doMain(args);
    }
    
    @Override
    protected void preProcess(){
        
    }

    @Override
    protected Callable<Void> getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File outputFile = new File(inputFile.getParentFile(), outputFilename);
        return new SampleTabToLoadRunnable(inputFile, outputFile);
    }
}
