package uk.ac.ebi.fgpt.sampletab.emblbank;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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
    
    @Option(name = "--wgs", usage = "is whole genome shotgun input?")
    private boolean wgs = false;
    
    @Option(name = "--tsa", usage = "is transcriptome shotgun input?")
    private boolean tsa = false;
    
    @Option(name = "--bar", usage = "is barcode input?")
    private boolean bar = false;
    
    @Option(name = "--cds", usage = "is coding sequence input?")
    private boolean cds = false;

    
    @Option(name = "-g", usage = "grouping filename")
    private String groupFilename = null;
    
    @Option(name = "-i", usage = "grouping index")
    private int groupIndex = 0;
    
    @Option(name = "-j", usage = "grouping offset")
    private int groupOffset = 0;
    

    private Logger log = LoggerFactory.getLogger(getClass());

    Pattern latLongPattern = Pattern.compile("([0-9]+\\.?[0-9]*) ([NS]) ([0-9]+\\.?[0-9]*) ([EW])");
    
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
        
        //read the file through once, construct mapping of pub to acc
        //read the file again, parsing each acc to a node
        //when all the nodes of a pub are parsed, output to file
        
        File groupFile = null; ;
        if (groupFilename != null) {
            groupFile = new File(groupFilename);
        }
        EMBLBankGrouper grouper = new EMBLBankGrouper(groupFile, groupMap, bar);
        
        if (groupIndex == 0) {
            //if a group index is specified, then we don't need to create the grouping file
            grouper.process(inputFile);
            
            if (groupFile != null) {
                //wrote groupings to a file,
                //end here
                return;
            }
        } else {
            //need to read existing group file to populate groupMap
            grouper.readGrouping(groupIndex, groupOffset);
        }
        
        log.info("Beginning second pass");

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (String groupID : groupMap.keySet()){
            Runnable t = new EMBLBankRunnable(inputFile, groupMap, groupID, outputFile, prefix, wgs, tsa, bar, cds);
            pool.execute(t);
        }
        
                // run the pool and then close it afterwards
        // must synchronize on the pool object
        synchronized (pool) {
            pool.shutdown();
            try {
                long starttime = System.currentTimeMillis();
                int startcount = groupMap.keySet().size();
                DateFormat dateformat = new SimpleDateFormat();
                while (!pool.awaitTermination(1, TimeUnit.SECONDS)){
                    int pendingcount = groupMap.keySet().size();
                    float percentagedone = new Float(startcount-pendingcount) / new Float(startcount);
                    long totaltime = (long) (new Float(System.currentTimeMillis()-starttime) / percentagedone);
                    Date enddate = new Date(starttime+totaltime);
                    log.info("Waiting on "+pendingcount+" groups, ETA "+dateformat.format(enddate));
                }
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
