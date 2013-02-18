package uk.ac.ebi.fgpt.sampletab.emblbank;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import au.com.bytecode.opencsv.CSVReader;

public class EMBLBankDriver {

    @Option(name = "-h", usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filename")
    private String inputFilename;
    private File inputFile;

    @Argument(required=true, index=1, metaVar="OUTPUT", usage = "output directory filename")
    private String outputFilename;
    private File outputFile;

    @Option(name = "-p", usage = "prefix")
    private String prefix = "GEM";
    
    @Option(name = "-wgs", usage = "is whole genome shotgun input?")
    private boolean wgs = false;
    
    @Option(name = "-tsa", usage = "is transcriptome shotgun input?")
    private boolean tsa = false;
    
    @Option(name = "-bar", usage = "is barcode input?")
    private boolean bar = false;
    
    @Option(name = "-cds", usage = "is coding sequence input?")
    private boolean cds = false;

    private Logger log = LoggerFactory.getLogger(getClass());

    Pattern latLongPattern = Pattern.compile("([0-9]+\\.?[0-9]*) ([NS]) ([0-9]+\\.?[0-9]*) ([EW])");
    
    private EMBLBankHeaders headers = null;
    int doiindex = -1;
    int pubmedindex = -1;
    
    int projheaderindex = -1;
    int pubheaderindex = -1;
    int collectedbyindex = -1;
    int identifiedbyindex = -1;
    int taxidindex = -1;

    Map<String, Set<String>> groupMap = new ConcurrentHashMap<String, Set<String>>();
    Map<String, Set<Publication>> publicationMap = new ConcurrentHashMap<String, Set<Publication>>();
    Map<String, SampleData> stMap = new ConcurrentHashMap<String, SampleData>();
    
    
    private void parseInput(File inputFile) throws IOException {

        CSVReader reader = null;
        
        
        //read the file through once, construct mapping of pub to acc
        //read the file again, parsing each acc to a node
        //when all the nodes of a pub are parsed, output to file
        
        EMBLBankRunnable dummy = null;
        
        
        int linecount;
        String [] nextLine;
        
        try {
            reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
            linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                                
                if (this.headers == null || linecount == 0){
                    this.headers = new EMBLBankHeaders(nextLine);
                    dummy = new EMBLBankRunnable(headers, nextLine, groupMap, publicationMap, stMap, outputFile, prefix, wgs, tsa, bar, cds);
                } else {
                    if (nextLine.length > headers.size()){
                        log.warn("Line longer than headers "+linecount+" ( "+nextLine.length+" vs "+headers.size()+" )");
                    }
                
                    String accession = nextLine[0].trim();
                    log.debug("First processing "+accession);
                    
                    for (String id : dummy.getGroupIdentifiers(nextLine)){

                        if(!groupMap.containsKey(id)){
                            groupMap.put(id, new HashSet<String>());
                        }
                        groupMap.get(id).add(accession);
                    }
                    
                    publicationMap.put(accession, dummy.getPublications(nextLine));
                    log.debug(accession+" "+dummy.getPublications(nextLine).size());
                    
                }
            }
            reader.close();
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
            }
        }

        log.info("First pass complete");
        log.info("No. of groups = "+groupMap.size());
        log.info("Beginning second pass");

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        
        try {
            reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
            linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                                
                if (this.headers == null || linecount == 0){
                    //do nothing, headers already loaded
                } else {
                    if (nextLine.length > this.headers.size()){
                        log.debug("Line longer than headers "+linecount+" ( "+nextLine.length+" vs "+headers.size()+" )");
                    }


                    Runnable t = new EMBLBankRunnable(headers, nextLine, groupMap, publicationMap, stMap, outputFile, prefix, wgs, tsa, bar, cds);
                    pool.execute(t);
                }
            }
            reader.close();
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
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
                log.error("Interupted awaiting thread pool termination", e);
            }
        }
    }

    public static void main(String[] args) {
        new EMBLBankDriver().doMain(args);
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

        inputFile = new File(inputFilename);
        if (!inputFile.exists()){
            log.error("Unable to load "+inputFilename);
            System.exit(2);
            return;
        }
        
        outputFile = new File(outputFilename);
        if (inputFile.exists() && !outputFile.isDirectory()){
            log.error("Unable to output to "+outputFilename);
            System.exit(3);
            return;
        }

        try {
            parseInput(inputFile);
        } catch (IOException e) {
            log.error("Unable to parse "+inputFilename, e);
        }       
    }
}
