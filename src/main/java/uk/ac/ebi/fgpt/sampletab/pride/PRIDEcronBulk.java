package uk.ac.ebi.fgpt.sampletab.pride;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dom4j.DocumentException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;
import uk.ac.ebi.fgpt.sampletab.SampleTabBulk;
import uk.ac.ebi.fgpt.sampletab.utils.PRIDEutils;

public class PRIDEcronBulk extends AbstractInfileDriver {


    //TODO make required
    @Option(name = "-j", aliases={"--projects"}, usage = "projects filename")
    private File projectsFile;
    

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
            File xml = new File(subdir, "trimmed.xml");
            File sampletabpre = new File(subdir, "sampletab.pre.txt");
            
            if (!xml.exists()) {
                log.warn("xml does not exist ("+xml+")");
                return;
            }
            
            File target;

            // convert xml to sampletab.pre.txt
            target = sampletabpre;
            if (!target.exists()
                    || target.length() == 0
                    || target.lastModified() < xml.lastModified()) {
                log.info("Processing " + target);
                
                PRIDEXMLToSampleTab c;
                try {
                    c = new PRIDEXMLToSampleTab(projectsFile.getAbsolutePath());
                    log.info("Converting "+xml.getPath()+" to "+sampletabpre.getPath());
                    c.convert(xml.getPath(), sampletabpre.getPath());
                } catch (IOException e) {
                    log.error("Problem processing "+sampletabpre, e);
                    return;
                } catch (DocumentException e) {
                    log.error("Problem processing "+sampletabpre, e);
                    return;
                } catch (ValidateException e) {
                    log.error("Problem processing "+sampletabpre, e);
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
        new PRIDEcronBulk().doMain(args);
    }

    @Override
    public void preProcess() {
        
        if (!scriptDir.exists() && !scriptDir.isDirectory()) {
            log.error("Script directory missing or is not a directory");
            System.exit(1);
            return;
        }
        log.info("Parsing projects file "+projectsFile);

        //read all the projects
        Map<String, Set<String>> projects;
        try {
            projects = PRIDEutils.loadProjects(projectsFile);
        } catch (IOException e) {
            log.error("Unable to read projects file "+projectsFile, e);
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
