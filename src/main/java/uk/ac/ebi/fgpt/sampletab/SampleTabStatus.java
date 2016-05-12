package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SampleTabStatus extends AbstractInfileDriver<SampleTabStatusRunnable> {

    @Option(name = "-f", aliases={"--ftp"}, usage = "ftp directory")
    private File ftpDirFile;
    

    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleTabStatus() {
        Properties properties = new Properties();
        try {
            properties.load(SampleTabStatus.class.getResourceAsStream("/sampletabconverters.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource sampletabconverters.properties", e);
        }
        ftpDirFile = new File(properties.getProperty("biosamples.path.ftp"));
    }
    
    public static void main(String[] args) {
        new SampleTabStatus().doMain(args);
    }

    @Override
    protected SampleTabStatusRunnable getNewTask(File inputFile) {
        return new SampleTabStatusRunnable(inputFile, ftpDirFile);
    }
}
