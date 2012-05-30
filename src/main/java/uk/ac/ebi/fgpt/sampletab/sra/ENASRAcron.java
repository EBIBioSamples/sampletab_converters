package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.dom4j.DocumentException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class ENASRAcron {

    @Option(name = "-h", aliases = { "--help" }, usage = "display help")
    private boolean help;

    @Option(name = "-o", aliases = { "--output" }, usage = "output directory")
    private String outputDirName;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;

    private Logger log = LoggerFactory.getLogger(getClass());

    private ENASRAcron() {
    }

    private class ENASRADownloadRunnable implements Runnable {
        private final File subdir;
        private final String identStudy;

        public ENASRADownloadRunnable(File subdir, String identStudy) {
            this.subdir = subdir;
            this.identStudy = identStudy;
        }

        public void run() {
            //TODO recycle these in a queue
            ENASRAWebDownload downloader = new ENASRAWebDownload();
            boolean newOrUpdate = false;
            try {
                newOrUpdate = downloader.download(this.identStudy, this.subdir);
            } catch (DocumentException e) {
                log.error("Unable to download "+this.identStudy+" to "+this.subdir);
                e.printStackTrace();
                return;
            } catch (IOException e) {
                log.error("Unable to download "+this.identStudy+" to "+this.subdir);
                e.printStackTrace();
                return;
            }
            if (!noconan && newOrUpdate) {
                String submissionIdentifier = "GEN-"+identStudy;
                try {
                    ConanUtils.submit(submissionIdentifier, "BioSamples (SRA) and load");
                } catch (IOException e) {
                    log.warn("Problem submitting "+submissionIdentifier);
                    e.printStackTrace();
                }
            }
        }
    }

    public void run(File outdir) {

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);


        //there isnt an easy way to get a list of all possible accessions
        //First though use the downloaded version from ftp.sra.ebi.ac.uk/meta/xml/xml.all.tar.gz
        //and go through those files.
        //But, this approach is broken because the filenames in the xml.all.tar.gz do not
        //match the accession numbers!
        //E.g. DRA000205 is actually DRA000206
        //solution is to crack open the files I find and check the REAL accession inside the XML
        //HORRIBLE!
        
        //Another approach: request an xml with ALL study accessions in using a wide range
        //http://www.ebi.ac.uk/ena/data/view/SRP000000-SRP900000&display=xml
        //http://www.ebi.ac.uk/ena/data/view/DNP000000-DNP900000&display=xml
        //http://www.ebi.ac.uk/ena/data/view/ERP000000-ERP900000&display=xml
        
        //...except that it wont let anyone download files that big. Grrr. <grumble>
        

        //BRUTE FORCE!
        for (int i = 0; i < 35000; i++) {
            String ident = String.format("SRP%06d", i);
            Runnable t = new ENASRADownloadRunnable(new File(outdir, "GEN-"+ident), ident);
            if (threaded) {
                pool.execute(t);
            } else {
                t.run();
            }
            
            if (i < 30000){
                ident = String.format("ERP%06d", i);
                t = new ENASRADownloadRunnable(new File(outdir, "GEN-"+ident), ident);
                if (threaded) {
                    pool.execute(t);
                } else {
                    t.run();
                }
                
            }
            
            if (i < 2000){
                ident = String.format("DRP%06d",i);
                t = new ENASRADownloadRunnable(new File(outdir, "GEN-"+ident), ident);
                if (threaded) {
                    pool.execute(t);
                } else {
                    t.run();
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
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        
		// TODO hide files that have disappeared from the FTP site.
	}

    public static void main(String[] args) {
        new ENASRAcron().doMain(args);
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
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        File outdir = new File(this.outputDirName);

        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Target is not a directory");
            System.exit(1);
            return;
        }

        if (!outdir.exists())
            outdir.mkdirs();

        this.run(outdir);
    }
}
