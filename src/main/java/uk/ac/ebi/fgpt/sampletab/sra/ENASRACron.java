package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ENASRACron {

    @Option(name = "-h", aliases = { "--help" }, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    private String outputDirName;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;

    private Logger log = LoggerFactory.getLogger(getClass());

    private ENASRACron() {
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
                log.error("Unable to download "+this.identStudy+" to "+this.subdir, e);
                return;
            } catch (IOException e) {
                log.error("Unable to download "+this.identStudy+" to "+this.subdir, e);
                return;
            }
            //Submit it to conan for processing
            if (!noconan && newOrUpdate) {
                String submissionIdentifier = "GEN-"+identStudy;
                try {
                    ConanUtils.submit(submissionIdentifier, "BioSamples (SRA)");
                } catch (IOException e) {
                    log.warn("Problem submitting "+submissionIdentifier, e);
                }
            }
        }
    }
    
    private class ENASRADeleteRunnable implements Runnable {
        private final File subdir;

        public ENASRADeleteRunnable(File subdir) {
            this.subdir = subdir;
        }

        public void run() {
            String accession = subdir.getName();
            String sraaccession = accession.substring(4);
            log.debug("Processing study "+accession);

            
            String url = "http://www.ebi.ac.uk/ena/data/view/" + sraaccession + "&display=xml";
            log.debug("Prepared for download "+url);

            Document studyDoc = null;
            try {
                studyDoc = XMLUtils.getDocument(url);
            } catch (DocumentException e) {
                log.error("Unable to read "+url, e);
            }
            
            if (studyDoc != null){
                Element root = studyDoc.getRootElement();
                if (root == null){
                    throw new RuntimeException("root is null");
                }
                //if this is a blank study, delete it
                if (XMLUtils.getChildrenByName(root, "STUDY").size() == 0) {

                    File sampletabFile = new File(subdir, "sampletab.pre.txt");
                    if (sampletabFile.exists()){
                        log.info("Deleted study "+accession);
                        boolean doConan = false;
                        try {
                            SampleTabUtils.releaseInACentury(sampletabFile);
                            doConan = true;
                        } catch (IOException e) {
                            log.error("Problem with "+sampletabFile, e);
                        } catch (ParseException e) {
                            log.error("Problem with "+sampletabFile, e);
                        }
                        //trigger conan to complete processing
                        if (!noconan && doConan) {
                            try {
                                ConanUtils.submit(accession, "BioSamples (SRA)", 1);
                            } catch (IOException e) {
                                log.warn("Problem submitting to Conan "+accession, e);
                            }
                        }
                    }
                }
            }
            
        }
    }

    public void run(File outdir) {


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

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
        

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
                log.error("Interuppted awaiting thread pool termination", e);
            }
        }
        
        pool = Executors.newFixedThreadPool(nothreads);
        
        log.info("Starting deleted processing");
        File[] stsubdirs = outdir.listFiles();
        Arrays.sort(stsubdirs);
        for(File subdir : stsubdirs){
            Runnable t = new ENASRADeleteRunnable(subdir);
            if (threaded) {
                pool.execute(t);
            } else {
                t.run();
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

    public static void main(String[] args) {
        new ENASRACron().doMain(args);
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
