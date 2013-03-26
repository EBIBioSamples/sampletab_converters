package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dom4j.DocumentException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;
import uk.ac.ebi.fgpt.sampletab.SampleTabBulk;
import uk.ac.ebi.fgpt.sampletab.utils.FileGlobIterable;

public class ENASRABulk extends AbstractInfileDriver {

    @Option(name = "--scripts", aliases={"-c"}, usage = "script directory")
    private File scriptDir;

    @Option(name = "--hostname", aliases={"-n"}, usage = "server hostname")
    private String hostname;

    @Option(name = "--port", usage = "server port")
    private Integer port = null;

    @Option(name = "--database", aliases={"-d"}, usage = "server database")
    private String database = null;

    @Option(name = "--username", aliases={"-u"}, usage = "server username")
    private String username = null;

    @Option(name = "--password", aliases={"-p"}, usage = "server password")
    private String password  = null;
    
    @Option(name = "--force", aliases={"-f"}, usage = "overwrite targets")
    private boolean force = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());


    private SampleTabBulk stcb = null;
    
    private class DoProcessFile implements Runnable {
        private final File subdir;
        
        public DoProcessFile(File subdir){
            this.subdir = subdir;
        }

        public void run() {
            String groupFilename = subdir.getName().substring(4)+".xml";
            File xmlFile = new File(subdir, groupFilename);
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            
            
            if (!xmlFile.exists()) {
                return;
            }
            
            File target;
            
            target = sampletabpre;
            if (!target.exists()
                    || target.length() == 0
                    || target.lastModified() < xmlFile.lastModified()) {
                log.info("Processing " + target);
                // convert study.xml to sampletab.pre.txt

                try {
                    new ENASRAXMLToSampleTab().convert(xmlFile, sampletabpre);
                } catch (IOException e) {
                    log.error("Problem processing "+xmlFile, e);
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+xmlFile, e);
                    return;
                } catch (DocumentException e) {
                    log.error("Problem processing "+xmlFile, e);
                    return;
                } catch (RuntimeException e) {
                    log.error("Problem processing "+xmlFile, e);
                    return;
                } 
                
            }
            
            if (stcb == null){
                stcb = new SampleTabBulk(hostname, port, database, username, password, force);
            }
            stcb.process(subdir, scriptDir);
        }
        
    }
    
    public static void main(String[] args) {
        new ENASRABulk().doMain(args);
    }

    @Override
    public void preProcess() {
        
        if (!scriptDir.exists() && !scriptDir.isDirectory()) {
            log.error("Script directory missing or is not a directory");
            System.exit(1);
            return;
        }
    }

    @Override
    protected Runnable getNewTask(File inputFile) {
        File subdir = inputFile.getAbsoluteFile().getParentFile();
        return new DoProcessFile(subdir);
    }
}
