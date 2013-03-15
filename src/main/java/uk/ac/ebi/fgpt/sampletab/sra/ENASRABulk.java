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
import uk.ac.ebi.fgpt.sampletab.SampleTabBulk;
import uk.ac.ebi.fgpt.sampletab.utils.FileGlobIterable;

public class ENASRABulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="SCRIPTDIR", usage = "script directory")
    private String scriptDirname;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filenames or globs")
    private List<String> inputFilenames;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "-n", aliases={"--hostname"}, usage = "Accession server hostname")
    private String hostname = null;

    @Option(name = "-t", aliases={"--port"}, usage = "Accession server port")
    private Integer port = null;

    @Option(name = "-d", aliases={"--database"}, usage = "Accession server database")
    private String database = null;

    @Option(name = "-u", aliases={"--username"}, usage = "Accession server username")
    private String username = null;

    @Option(name = "-p", aliases={"--password"}, usage = "Accession server password")
    private String password = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());


    private SampleTabBulk stcb = null;
    
    private class DoProcessFile implements Runnable {
        private final File subdir;
        private final File scriptdir;
        
        public DoProcessFile(File subdir, File scriptdir){
            this.subdir = subdir;
            this.scriptdir = scriptdir;
        }

        public void run() {
            String studyFilename = "study.xml";
            File xmlFile = new File(subdir, studyFilename);
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
                stcb = new SampleTabBulk(hostname, port, database, username, password);
            }
            stcb.process(subdir, scriptdir);
        }
        
    }
    
    public static void main(String[] args) {
        new ENASRABulk().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        

        File scriptdir = new File(scriptDirname);
        scriptdir = scriptdir.getAbsoluteFile();

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);

        for (String inputFilename : inputFilenames){
            for(File subdir : new FileGlobIterable(inputFilename)){
                if (subdir.isDirectory()) {
                    Runnable t = new DoProcessFile(subdir, scriptdir);
                    if (threaded) {
                        pool.execute(t);
                    } else {
                        t.run();
                    }
                }
            }
        }
                
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interuppted awaiting thread pool termination", e);
            }
        }
    }
}
