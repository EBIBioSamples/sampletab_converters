package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class NCBIBiosampleDriver extends AbstractInfileDriver<NCBIBiosampleRunnable> {
    
    @Option(name = "-o", usage = "output filename")
    private String outputFilename;
    
	private Logger log = LoggerFactory.getLogger(getClass());

	public NCBIBiosampleDriver() {
	    
	}

    @Override
    protected NCBIBiosampleRunnable getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File outputFile = new File(inputFile.getParentFile(), outputFilename);
        return new NCBIBiosampleRunnable(inputFile, outputFile);
    }
}
