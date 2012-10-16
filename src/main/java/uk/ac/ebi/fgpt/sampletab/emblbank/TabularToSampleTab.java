package uk.ac.ebi.fgpt.sampletab.emblbank;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonException;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonUtils;

import au.com.bytecode.opencsv.CSVReader;

public class TabularToSampleTab {

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

    private Logger log = LoggerFactory.getLogger(getClass());

    private TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    Pattern latLongPattern = Pattern.compile("([0-9]+\\.?[0-9]*) ([NS]) ([0-9]+\\.?[0-9]*) ([EW])");
    
    private List<String> headers = null;
          
    private SampleNode lineToSample(String[] nextLine){

        SampleNode s = new SampleNode();
        
        for (int i = 0; i < nextLine.length && i < headers.size(); i++){
            String header = headers.get(i).trim();
            String value = nextLine[i].trim();
            if (value != null && value.length() > 0){
                log.debug(header+" : "+value);
                
                if (header.equals("ACCESSION") || header.equals("ACC")){
                    s.setNodeName(value);
                } else if (header.equals("ORGANISM")){
                    if (value.contains(" BOLD:")){
                        //remove BOLD:xxxxxx 
                        //BOLD = Barcode Of Life Database
                        String bold = value.substring(value.indexOf(" BOLD:"));
                        bold = bold.trim();
                        value = value.substring(0, value.indexOf(" BOLD:"));
                        s.addAttribute(new OrganismAttribute(value));

                        s.addAttribute(new DatabaseAttribute("BOLD", bold, "http://www.boldsystems.org/index.php/Public_BarcodeCluster?clusterguid="+bold));
                        
                    } else {
                        //no Bold entry, just use organism
                        s.addAttribute(new OrganismAttribute(value));
                    }
                } else if (header.equals("BIO_MATERIAL")){
                    s.addAttribute(new CommentAttribute("Biomaterial", value));
                } else if (header.equals("BIOSEQID")){
                    s.addAttribute(new CommentAttribute("BioSeq ID", value));
                } else if (header.equals("CULTURE_COLLECTION")){
                    s.addAttribute(new CommentAttribute("Culture Collection", value));
                } else if (header.equals("SPECIMEN_VOUCHER")){
                    s.addAttribute(new CommentAttribute("Specimen Voucher", value));
                } else if (header.equals("COLLECTED_BY")){
                    s.addAttribute(new CommentAttribute("Collected By", value));
                } else if (header.equals("COLLECTION_DATE")){
                    //TODO handle date
                    s.addAttribute(new CommentAttribute("Collection Date", value));
                } else if (header.equals("COUNTRY")){
                    s.addAttribute(new CharacteristicAttribute("Geographic Origin", value));
                    //TODO split better
                } else if (header.equals("HOST")){
                    s.addAttribute(new CharacteristicAttribute("Host", value));
                } else if (header.equals("IDENTIFIED_BY")){
                    s.addAttribute(new CommentAttribute("Identified By", value));
                } else if (header.equals("ISOLATION_SOURCE")){
                    s.addAttribute(new CharacteristicAttribute("Isolation Source", value));
                } else if (header.equals("LAT_LON")){
                    Matcher m = latLongPattern.matcher(value);    
                    if (m.lookingAt()){
                        Float latitude = new Float(m.group(1));  
                        if (m.group(2).equals("S")){
                            latitude = -latitude;
                        }
                        s.addAttribute(new CharacteristicAttribute("Latitude", latitude.toString()));
                        //TODO units, decimal degrees
                        
                        Float longitude = new Float(m.group(3));  
                        if (m.group(4).equals("W")){
                            longitude = -longitude;
                        }
                        s.addAttribute(new CharacteristicAttribute("Longitude", latitude.toString()));
                        //TODO units, decimal degrees
                    } else {
                        log.warn("Unable to match "+header+" : "+value);
                    }
                } else if (header.equals("LAB_HOST")){
                    s.addAttribute(new CharacteristicAttribute("Lab Host", value));
                } else if (header.equals("ENVIRONMENTAL_SAMPLE")){
                    s.addAttribute(new CommentAttribute("Environmental?", value));
                } else if (header.equals("MATING_TYPE")){
                    s.addAttribute(new SexAttribute(value));
                } else if (header.equals("SEX")){
                    s.addAttribute(new SexAttribute(value));
                } else if (header.equals("CELL_TYPE")){
                    s.addAttribute(new CharacteristicAttribute("Cell Type", value));
                } else if (header.equals("DEV_STAGE")){
                    s.addAttribute(new CharacteristicAttribute("Developmental Stage", value));
                } else if (header.equals("GERMLINE")){
                    s.addAttribute(new CharacteristicAttribute("Germline", value));
                } else if (header.equals("TISSUE_LIB")){
                    s.addAttribute(new CharacteristicAttribute("Tissue Library", value));
                } else if (header.equals("TISSUE_TYPE")){
                    s.addAttribute(new CharacteristicAttribute("Organism Part", value));
                } else if (header.equals("CULTIVAR")){
                    s.addAttribute(new CharacteristicAttribute("Cultivar", value));
                } else if (header.equals("ECOTYPE")){
                    s.addAttribute(new CharacteristicAttribute("Ecotype", value));
                } else if (header.equals("ISOLATE")){
                    s.addAttribute(new CharacteristicAttribute("Isolate", value));
                } else if (header.equals("STRAIN")){
                    s.addAttribute(new CharacteristicAttribute("Strain or Line", value));
                } else if (header.equals("SUB_SPECIES")){
                    s.addAttribute(new CharacteristicAttribute("Sub Species", value));
                } else if (header.equals("VARIETY")){
                    s.addAttribute(new CharacteristicAttribute("Variety", value));
                } else if (header.equals("SUB_STRAIN")){
                    s.addAttribute(new CharacteristicAttribute("Sub Strain", value));
                } else if (header.equals("CELL_LINE")){
                    s.addAttribute(new CharacteristicAttribute("Strain or Line", value));
                } else if (header.equals("SEROTYPE")){
                    s.addAttribute(new CharacteristicAttribute("Serotype", value));
                } else if (header.equals("SEROVAR")){
                    s.addAttribute(new CharacteristicAttribute("Serovar", value));
                } else if (header.equals("ORGANELLE")){
                    s.addAttribute(new CharacteristicAttribute("Organelle", value));
                } else if (header.equals("PLASMID")){
                    s.addAttribute(new CharacteristicAttribute("Plasmid", value));
                } else if (header.equals("TAX_ID")){
                    //manipulate existing organism attribute
                    boolean found = false;
                    for (SCDNodeAttribute a : s.getAttributes()){
                        if (OrganismAttribute.class.isInstance(a)){
                            OrganismAttribute o = (OrganismAttribute) a;
                            o.setTermSourceREF(ncbitaxonomy.getName());
                            o.setTermSourceID(value);
                            found = true;
                        }
                    }
                    if (!found){
                        //no existing organism attribute, add one
                        log.info("looking up taxid "+value);
                        Integer taxID = new Integer(value);
                        OrganismAttribute o = null;
                        try {
                            o = new OrganismAttribute(TaxonUtils.getTaxonOfID(taxID));
                        } catch (TaxonException e) {
                            log.warn("Problem getting taxid of "+value, e);
                        } 
                        if (o == null){
                            o = new OrganismAttribute(value);
                        }
                        o.setTermSourceREF(ncbitaxonomy.getName());
                        o.setTermSourceID(value);
                        s.addAttribute(o);
                    }
                }
            }
        }
        
        s.addAttribute(new DatabaseAttribute("EMBL-Bank", s.getNodeName(), "http://www.ebi.ac.uk/ena/data/view/"+s.getNodeName()));
        
        return s;
    }
    
    private List<String> getGroupIdentifiers(String[] line){

        List<String> identifiers = new ArrayList<String>();
        
        int projheaderindex = headers.indexOf("PROJECT_ACC");
        int pubheaderindex = headers.indexOf("ENA_PUBID");
        
        String projString = null;
        projString = line[projheaderindex].trim();
        if (projString != null && projString.length() > 0){
            for(String projID : projString.split(",")){
                projID = projID.trim();
                if (projID.length() > 0){
                    identifiers.add(projID);
                }
            }
        } else {
            String pubString = null;
            pubString = line[pubheaderindex];
            for(String pubID : pubString.split(",")){
                pubID = pubID.trim();
                if (pubID.length() > 0){
                    identifiers.add(pubID);
                }
            }
        }
        if (identifiers.size() == 0){
            log.warn("No identifiers for "+line[0]);
        }
        return identifiers;
    }
    
    private void parseInput(File inputFile) throws IOException {

        CSVReader reader = null;
        
        
        //read the file through once, construct mapping of pub to acc
        //read the file again, parsing each acc to a node
        //when all the nodes of a pub are parsed, output to file
        
        Map<String, Set<String>> groupMap = new HashMap<String, Set<String>>();
        
        Map<String, SampleData> stMap = new HashMap<String, SampleData>();
        
        int linecount;
        String [] nextLine;
        
        try {
            reader = new CSVReader(new FileReader(inputFile));
            linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                                
                if (headers == null){
                    headers = new ArrayList<String>();
                    for (String header : nextLine){
                        log.info("Found header : "+header);
                        headers.add(header);
                    }
                } else {
                    if (nextLine.length > headers.size()){
                        log.warn("Line longer than headers "+linecount);
                    }
                
                    String accession = nextLine[0].trim();
                    
                    for (String id : getGroupIdentifiers(nextLine)){

                        if(!groupMap.containsKey(id)){
                            groupMap.put(id, new HashSet<String>());
                        }
                        groupMap.get(id).add(accession);
                    }
                    
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

        try {
            reader = new CSVReader(new FileReader(inputFile));
            linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                                
                if (headers == null){
                    headers = new ArrayList<String>();
                    for (String header : nextLine){
                        headers.add(header);
                    }
                } else {
                    if (nextLine.length > headers.size()){
                        log.warn("Line longer than headers "+linecount);
                    }
                    
                    SampleNode s = lineToSample(nextLine);
                    
                    if (s.getNodeName() == null){
                        log.error("Unable to add node with null name");
                        continue;
                    }
                    
                    //now we have a described node, but need to work out what publications to put it with

                    
                    for (String id : getGroupIdentifiers(nextLine)){
                        
                        if (!groupMap.containsKey(id)){
                            continue;
                        }
                        
                        if(!stMap.containsKey(id)){
                            stMap.put(id, new SampleData());
                        }
                    
                        SampleData st = stMap.get(id);
                        
                        try {
                            if (s.getNodeName() == null){
                                log.error("Unable to add node with null name");
                            } else if (st.scd.getNode(s.getNodeName(), SampleNode.class) != null) {
                                //this node name has already been used
                                log.error("Unable to add duplicate node with name "+s.getNodeName());
                            } else if (s.getAttributes().size() <= 1){
                                //dont add uninformative samples
                                //will always have one database attribute
                                log.warn("Refusing to add sample "+s.getNodeName()+" without attributes");
                            } else {
                                st.scd.addNode(s);
                            }
                        } catch (ParseException e) {
                            log.error("Unable to add node "+s.getNodeName(), e);
                        }     
                        
                        if (!groupMap.containsKey(id)){
                            log.error("Problem locating publication ID "+id+" in publication maping");
                            continue;
                        }
                        
                        if (!groupMap.get(id).contains(s.getNodeName())){
                            log.error("Problem removing sample "+s.getNodeName()+" from publication maping");
                            continue;
                        }
                        
                        groupMap.get(id).remove(s.getNodeName());
                        
                        if (groupMap.get(id).size() == 0){
                            //no more accessions to be added to this sampletab
                            //export it

                            //create some information
                            st.msi.submissionIdentifier = prefix+"-"+id;
                            if (wgs){
                                //its a whole genome shotgun sample, describe as such
                                String speciesName = null;
                                for (SampleNode sn : st.scd.getNodes(SampleNode.class)){
                                    for (SCDNodeAttribute a : sn.getAttributes()){
                                        if (OrganismAttribute.class.isInstance(a)){
                                            OrganismAttribute oa = (OrganismAttribute) a;
                                            speciesName = a.getAttributeValue();
                                        }
                                    }
                                }
                                if (speciesName == null){
                                    log.warn("Unable to determine species name");
                                } else {
                                    st.msi.submissionTitle = "Whole Genome Shotgun sequencing of "+speciesName;
                                }
                            }
                            
                            File outputSubDir = new File(outputFile, st.msi.submissionIdentifier);
                            File sampletabPre = new File(outputSubDir, "sampletab.pre.txt");
                            outputSubDir.mkdirs();
                            log.debug("Writing "+sampletabPre);
                            SampleTabWriter sampletabwriter = null;
                            try {
                                sampletabwriter = new SampleTabWriter(new BufferedWriter(new FileWriter(sampletabPre)));
                                sampletabwriter.write(st);
                            } catch (IOException e) {
                                log.error("Unable to write to "+sampletabPre, e);
                            } finally {
                                if (sampletabwriter != null){
                                    try {
                                        sampletabwriter.close();
                                    } catch (IOException e) {
                                        //do nothing
                                    }
                                }
                            }
                            
                            //remove from mappings
                            groupMap.remove(id);
                            stMap.remove(id);
                        }
                        
                    }
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
    }

    public static void main(String[] args) {
        new TabularToSampleTab().doMain(args);
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
