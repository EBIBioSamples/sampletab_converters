package uk.ac.ebi.fgpt.sampletab.other;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

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
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileGlobIterable;

public class CoriellFamily {

    @Option(name = "-h", aliases = { "--help" }, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases = { "--input" }, usage = "glob of input files")
    private String inputFilename;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private final SampleTabSaferParser parser = new SampleTabSaferParser();
    
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
                if (relationship == null) {
                    // do nothing
                } else if(relationship.equals("daughter") 
                        || relationship.equals("son") 
                        || relationship.equals("child")) {
                    relationship = "child";
                } else if (relationship.equals("brother")
                        || relationship.equals("sister") 
                        || relationship.equals("sibling")) {
                    relationship = "sibling";
                } else if (relationship.equals("half-brother")
                        || relationship.equals("half-sister") 
                        || relationship.equals("half-sibling")) {
                    relationship = "half-sibling";
                } else if (relationship.equals("twin brother")
                        || relationship.equals("twin sister") 
                        || relationship.equals("twin")) {
                    relationship = "twin";
                } else if (relationship.equals("identical twin brother")
                        || relationship.equals("identical twin sister") 
                        || relationship.equals("identical twin")) {
                    relationship = "identical twin";
                } else if (relationship.equals("father")
                        || relationship.equals("mother") 
                        || relationship.equals("parent")) {
                    relationship = "parent";
                } else if (relationship.equals("step-father")
                        || relationship.equals("step-mother") 
                        || relationship.equals("step-parent")) {
                    relationship = "step-parent";
                } else if (relationship.equals("husband")
                        || relationship.equals("wife") 
                        || relationship.equals("unaffected spouse")
                        || relationship.equals("spouse")) {
                    relationship = "spouse";
                } else {
                    relationship = null;
                }
                if (relationship != null){
                    sample.addAttribute(new CharacteristicAttribute(relationship, probandName));  
                    log.info("Adding "+relationship+" of "+probandName+" to "+sample.getNodeName());
                }
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
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        
        log.debug("Looking for input files");

        for(File inputFile : new FileGlobIterable(inputFilename)){
            SampleData st = null;
            try {
                st = this.parser.parse(inputFile);
            } catch (ParseException e) {
                log.error("ParseException converting " + inputFile, e);
                System.exit(1);
            }
            
            if (st != null) {
                process(st);
            }

        }
        
    }

}
