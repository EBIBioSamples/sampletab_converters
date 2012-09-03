package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.SampleTabStatus;
import uk.ac.ebi.fgpt.sampletab.utils.AgeUtils;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class TagBulkDriver {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;


    @Argument(required=true, index=0, metaVar="SCRIPT", usage = "script directory")
    private String scriptDirFilename;
    
    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;

    @Option(name = "--agename", usage = "Age server hostname")
    private String agehostname; //default set in constructor

    @Option(name = "--ageusername", usage = "Age server username")
    private String ageusername; //default set in constructor

    @Option(name = "--agepassword", usage = "Age server password")
    private String agepassword; //default set in constructor

    private Logger log = LoggerFactory.getLogger(getClass());

    public TagBulkDriver(){
        Properties ageProperties = new Properties();
        try {
            ageProperties.load(SampleTabStatus.class.getResourceAsStream("/age.properties"));
        } catch (IOException e) {
            log.error("Unable to read resource age.properties", e);
        }
        this.agehostname = ageProperties.getProperty("hostname");
        this.ageusername = ageProperties.getProperty("username");
        this.agepassword = ageProperties.getProperty("password");
    }
    
    public static void main(String[] args) {
        new TagBulkDriver().doMain(args);
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
        

        log.info("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
        }
        log.info("Found " + inputFiles.size() + " input files");
        
        //no duplicates
        Set<File> inputFileSet = new HashSet<File>();
        for (File inputFile : inputFiles) {
            if (!inputFile.isDirectory()) {
                inputFile = inputFile.getAbsoluteFile().getParentFile();
            }
            if (inputFile != null) {
                inputFileSet.add(inputFile);
            }
        }
        inputFiles.clear();
        inputFiles.addAll(inputFileSet);
        
        //sort to get consistent order
        Collections.sort(inputFiles);
        
        log.info("Found " + inputFiles.size() + " input files");
        
        //convert to strings by filename
        List<String> inputAccessions = new ArrayList<String>(inputFiles.size());
        for (int i = 0; i < inputFiles.size(); i++){
            inputAccessions.add(i, inputFiles.get(i).getName());
        }
        
        AgeUtils ageUtils = new AgeUtils(scriptDirFilename, ageusername, agepassword, agehostname);
        
        Map<String, Set<String>> tags = ageUtils.BulkTagQuery(inputAccessions);
        
        for(String accession : inputAccessions){
            if (tags.containsKey(accession)){
                log.info(accession+" : "+tags.get(accession));
            }
        }
        
    }
}
