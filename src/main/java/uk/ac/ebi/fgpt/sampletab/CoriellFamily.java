package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class CoriellFamily {

    @Option(name = "-h", aliases = { "--help" }, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases = { "--input" }, usage = "glob of input files")
    private String inputFilename;

    @Option(name = "-o", aliases = { "--output" }, usage = "output directory")
    private String outputFilename;

    private Logger log = LoggerFactory.getLogger(getClass());


    private final SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>();
    
    public static void main(String[] args) {
        new CoriellFamily().doMain(args);
    }
    
    public void process(SampleData st){
        if (st == null)
            throw new IllegalArgumentException("st must not be null");
        
        Map<String, String> probands = new HashMap<String,String>();

        for (SampleNode sample : st.scd.getNodes(SampleNode.class)){
            String familyID = null;
            String relationship = null;
            for (SCDNodeAttribute a : sample.getAttributes()){
                log.trace(a.getAttributeType());
                if (a.getAttributeType().equals("characteristic[Family Identifier]")){
                    familyID = a.getAttributeValue();
                }
                if (a.getAttributeType().equals("characteristic[Family Relationship]")){
                    relationship = a.getAttributeValue();
                }
            }
            if (familyID != null && familyID.length() > 0){
                if (relationship != null && relationship.equals("proband")){
                    probands.put(familyID, sample.getNodeName());
                    log.info("found proband "+sample.getNodeName());
                }
            }
        }
        for (SampleNode sample : st.scd.getNodes(SampleNode.class)){
            String familyID = null;
            String relationship = null;
            for (SCDNodeAttribute a : sample.getAttributes()){
                if (a.getAttributeType().equals("characteristic[Family Identifier]")){
                    familyID = a.getAttributeValue();
                }
                if (a.getAttributeType().equals("characteristic[Family Relationship]")){
                    relationship = a.getAttributeValue();
                }
            }
            if (familyID != null && familyID.length() > 0 && probands.containsKey(familyID)){
                String probandName = probands.get(familyID);
                //TODO only bother for a very specific sub-set of relationships
                sample.addAttribute(new CharacteristicAttribute(relationship, probandName));  
                log.info("Adding "+relationship+" of "+probandName+" to "+sample.getNodeName());
            }
        }
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
        
        log.debug("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files from "+inputFilename);
        Collections.sort(inputFiles);

        for(File inputFile : inputFiles){
            SampleData st = null;
            try {
                st = this.parser.parse(inputFile);
            } catch (ParseException e) {
                System.err.println("ParseException converting " + inputFile);
                e.printStackTrace();
                System.exit(1);
            }
            
            if (st != null) {
                process(st);
            }

        }
        
    }

}
