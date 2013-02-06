package uk.ac.ebi.fgpt.sampletab.arrayexpress;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.fgpt.sampletab.SampleTabBulk;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class MageTabBulk {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="SCRIPTDIR", usage = "script directory")
    private String scriptDirname;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filenames or globs")
    private List<String> inputFilenames;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "-n", aliases={"--hostname"}, usage = "MySQL server hostname")
    private String hostname = null;

    @Option(name = "-t", aliases={"--port"}, usage = "MySQL server port")
    private Integer port = null;

    @Option(name = "-d", aliases={"--database"}, usage = "MySQL server database")
    private String database = null;

    @Option(name = "-u", aliases={"--username"}, usage = "MySQL server username")
    private String username = null;

    @Option(name = "-p", aliases={"--password"}, usage = "MySQL server password")
    private String password = null;

    @Option(name = "--agename", usage = "Age server hostname")
    private String agename = null;

    @Option(name = "--ageusername", usage = "Age server username")
    private String ageusername = null;

    @Option(name = "--agepassword", usage = "Age server password")
    private String agepassword = null;

    @Option(name = "--no-load", usage = "Do not load into Age")
    private boolean noload = false;
    
    
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
            String idffilename = (subdir.getName().replace("GAE-", "E-")) + ".idf.txt";
            String sdrffilename = (subdir.getName().replace("GAE-", "E-")) + ".sdrf.txt";
            File idffile = new File(subdir, idffilename);
            File sdrffile = new File(subdir, sdrffilename);
            File sampletabpre = new File(subdir, "sampletab.pre.txt");

            //TODO fix the few files that cannot be processed
            if (idffilename.equals("E-GEOD-27923.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is too large");
                return;
            } else if (idffilename.equals("E-GEOD-21478.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is too large");
                return;
            } else if (idffilename.equals("E-GEOD-9376.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is too large");
                return;
            } else if (idffilename.equals("E-GEOD-28791.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is too large");
                return;
            } else if (idffilename.equals("E-MEXP-1769.idf.txt")){
                log.warn("Skipping "+idffilename+" as its sdrf requries merging which is not currently supported.");
                return;
            } else if (idffilename.equals("E-MEXP-2469.idf.txt")){
                log.warn("Skipping "+idffilename+" as its sdrf requries merging which is not currently supported.");
                return;
            } else if (idffilename.equals("E-MEXP-2622.idf.txt")){
                log.warn("Skipping "+idffilename+" as its sdrf requries merging which is not currently supported.");
                return;
            } else if (idffilename.equals("E-GEOD-14511.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-15443.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-15448.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-16375.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-17067.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-17732.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-18069.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-19892.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-19986.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-20076.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-20418.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-20753.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-21068.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-21202.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-21671.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-21790.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-21978.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-22105.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-22341.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-7788.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-9344.idf.txt")){
                log.warn("Skipping "+idffilename+" as it is has non-standard sdrfs");
                return;
            } else if (idffilename.equals("E-GEOD-10348.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-12578.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-15186.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-16159.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-16579.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-19553.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-20668.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-21161.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-21242.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-21427.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-21812.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22067.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22410.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22478.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22657.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22763.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-22959.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-23316.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-23762.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-24565.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-25124.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-25308.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-26064.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-26284.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-26367.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-27221.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-28264.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-28269.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-28919.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-29362.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-30017.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-30171.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-30538.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-30724.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-31052.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-31211.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-31226.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-32045.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-33178.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-33213.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-33584.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-33600.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-34399.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-34415.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-35806.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-36029.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-37650.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-37858.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-37909.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-38575.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-39977.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-40158.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-40727.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-40832.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-GEOD-5516.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-CBIL-28.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-BUGS-62.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-1671.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-268.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-3491.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-43.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-71.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MEXP-938.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MIMR-282.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MIMR-461.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MIMR-541.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-202.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-345.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-463.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-678.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-730.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-MTAB-777.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-NASC-8.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-TABM-213.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-TABM-294.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-TABM-443.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } else if (idffilename.equals("E-TOXM-40.idf.txt")){
                log.warn("Skipping "+idffilename+" as it does not parse correctly");
                return;
            } 
            
            if (!idffile.exists() || !sdrffile.exists()) {
                return;
            }
            
            File target;

            // convert idf/sdrf to sampletab.pre.txt
            target = sampletabpre;
            if (!target.exists() 
                    || target.length() == 0
                    || target.lastModified() < idffile.lastModified() 
                    || target.lastModified() < sdrffile.lastModified()) {
                log.info("Processing " + target);
                
                
                try {
                    new MageTabToSampleTab().convert(idffile, sampletabpre);
                } catch (IOException e) {
                    log.error("Problem processing "+idffile, e);
                    return;
                } catch (ParseException e) {
                    log.error("Problem processing "+idffile, e);
                    return;
                } catch (RuntimeException e) {
                    log.error("Problem processing "+idffile, e);
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
        new MageTabBulk().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
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
            for (File subdir : FileUtils.getMatchesGlob(inputFilename)){
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
