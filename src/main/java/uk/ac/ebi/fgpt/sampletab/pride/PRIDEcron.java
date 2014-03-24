package uk.ac.ebi.fgpt.sampletab.pride;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.dom4j.DocumentException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.fgpt.sampletab.SampleTabBulkDriver;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class PRIDEcron {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
    private File outputDir;

    @Option(name = "--threads", aliases={"-t"}, usage = "number of additional threads")
    private int threads = 0;

    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;

    @Option(name = "--migrate-accession", usage = "migrate accession numbers?")
    private boolean migrateaccession = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private FTPClient ftp = null;
    
    private Set<String> updated = new HashSet<String>();
    
    private Set<String> deleted = new HashSet<String>();

    private Map<String, Set<String>> groups = new HashMap<String, Set<String>>();

    private PRIDEcron() {
        
    }
    
    private File getTrimFile(File outDir, String experiment, String accession) {
        File experimentDir = new File(outDir, "GPR-"+experiment);
        File trimFile = new File(experimentDir, accession+".xml");
        return trimFile;
    }
    
    private void downloads() {

        Pattern regex = Pattern.compile("PRIDE_Exp_Complete_Ac_([0-9]+)\\.xml\\.gz");
                
        ExecutorService pool = null;
        if (threads > 0 ) {
            pool = Executors.newFixedThreadPool(threads);
        }
        
        
        try {
            ftp = FTPUtils.connect("ftp.pride.ebi.ac.uk");
            log.info("Getting file listing...");
            FTPFile[] years = ftp.listDirectories("/pride/data/archive");
            for (FTPFile year : years) {
                log.trace("found year "+year.getName());
                FTPFile[] months = ftp.listDirectories("/pride/data/archive/"+year.getName());
                for (FTPFile month : months) {
                    log.trace("found month "+month.getName());
                    //TODO something
                    FTPFile[] experiments = ftp.listFiles("/pride/data/archive/"+year.getName()+"/"+month.getName());
                    for (FTPFile experiment: experiments) {
                        String experimentID = experiment.getName();
                        log.trace("checking experiment "+experimentID);
                        FTPFile[] files = ftp.listFiles("/pride/data/archive/"+year.getName()+"/"+month.getName()+"/"+experimentID);
                        for (FTPFile file : files) {
                            //do a regular expression to match and pull out accession
                            Matcher matcher = regex.matcher(file.getName());
                            log.trace("/pride/data/archive/"+year.getName()+"/"+month.getName()+"/");
                            
                            if (matcher.matches()) {
                                String accession = matcher.group(1);
                                //add it to the grouping
                                if (!groups.containsKey(experimentID)) { 
                                    groups.put(experimentID, new HashSet<String>());
                                }
                                groups.get(experimentID).add(accession);
                                //download it to the update
                                File outfile = getTrimFile(outputDir, experimentID, accession);
                                // do not overwrite existing files unless newer
                                Calendar ftptime = file.getTimestamp();
                                Calendar outfiletime = new GregorianCalendar();
                                outfiletime.setTimeInMillis(outfile.lastModified());
                                if (!outfile.exists() || ftptime.after(outfiletime)) {
                                    Runnable t = new PRIDEXMLFTPDownload("/pride/data/archive/"
                                            +year.getName()+"/"+month.getName()+"/"
                                            +experimentID+"/"+file.getName(), outfile);
                                    if (threads > 0) {
                                        pool.execute(t);
                                    } else {
                                        t.run();
                                    }
                                    updated.add(experimentID);
                                }

                                //check the sampletab.pre.txt file exists, and if not flag an update
                                File sampleTabFile = new File(new File(outputDir, "GPR-"+experimentID), "sampletab.pre.txt");
                                if (!sampleTabFile.exists()) {
                                    updated.add(experimentID);
                                }
                            }
                        }
                    }
                    
                }
            }
            log.info("Got PRIDE FTP file listing");
        } catch (IOException e) {
            log.error("Unable to connect to FTP", e);
            System.exit(1);
            return;
        } finally {
            try {
                ftp.logout();
            } catch (IOException e) {
                if (ftp.isConnected()) {
                    try {
                        ftp.disconnect();
                    } catch (IOException ioe) {
                        // do nothing
                    }
                }
            }
        }

        /*
        //output a mapping of groupings
        for (String group : groups.keySet()) {
            String line = group;
            List<String> accessions = new ArrayList<String>(groups.get(group));
            Collections.sort(accessions);
            for (String accession : accessions) {
                line = line+"\t"+accession;
            }
            //System.out.println(line);
        }
        */

        if (migrateaccession) {
            //output a dummy test of migrating the accessions
            //load defaults from file
            //will be overridden by command-line options later
            Properties properties = new Properties();
            try {
                InputStream is = SampleTabBulkDriver.class.getResourceAsStream("/oracle.properties");
                properties.load(is);
            } catch (IOException e) {
                log.error("Unable to read resource oracle.properties", e);
            }
            
            String hostname = properties.getProperty("hostname");
            Integer port = new Integer(properties.getProperty("port"));
            String database = properties.getProperty("database");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            PRIDEmigrateaccession migrate = new PRIDEmigrateaccession(groups, hostname, port, database, username, password);
            migrate.process();
        }
        
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        if (pool != null) {
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

    public static void main(String[] args) {
        new PRIDEcron().doMain(args);
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
        
        if (outputDir.exists() && !outputDir.isDirectory()) {
            System.err.println("Target is not a directory");
            System.exit(1);
            return;
        }

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        downloads();
        log.info("Completed downloading, starting parsing...");
        //TODO hide files that have disappeared from the FTP site.            
        
        ExecutorService pool = null;
        if (threads > 0 ) {
            pool = Executors.newFixedThreadPool(threads);
        
            Collection<Future<Void>> futures = new ArrayList<Future<Void>>();
            for (String experimentID: updated) {
                Set<File> files = new HashSet<File>();
                for (String accession : groups.get(experimentID)) {
                    files.add(getTrimFile(outputDir, experimentID, accession));
                }
                
                File outFile = SampleTabUtils.getSubmissionDirFile("GPR-"+experimentID);
                outFile = new File(outFile, "sampletab.pre.txt");
                
                PRIDEXMLCallable callable = new PRIDEXMLCallable(files, outFile);
                futures.add(pool.submit(callable));
            }
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    //something went wrong
                    log.error("problem getting future", e);
                } catch (ExecutionException e) {
                    //something went wrong
                    log.error("problem getting future", e);
                }
            }
            // run the pool and then close it afterwards
            // must synchronize on the pool object
            if (pool != null) {
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
        } else {
            //not threaded
            for (String experimentID: updated) {
                Set<File> files = new HashSet<File>();
                for (String accession : groups.get(experimentID)) {
                    files.add(getTrimFile(outputDir, experimentID, accession));
                }
                
                File outFile = SampleTabUtils.getSubmissionDirFile("GPR-"+experimentID);
                outFile = new File(outFile, "sampletab.pre.txt");
                
                PRIDEXMLCallable callable = new PRIDEXMLCallable(files, outFile);
                try {
                    callable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!noconan) {
            //submit to conan
            for (String submission : updated) {
                try {
                    ConanUtils.submit(submission, "BioSamples (other)");
                } catch (IOException e) {
                    log.error("Problem submitting "+submission+" to conan", e);
                }
            }
        }
    }
}
