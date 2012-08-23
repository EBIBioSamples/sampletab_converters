package uk.ac.ebi.fgpt.sampletab.emblbank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
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
    

    private Logger log = LoggerFactory.getLogger(getClass());
    
    private int outcount = 0;
    private final int samplecount = 10000;
   
    private SampleData getNewSampleData(){
        SampleData st = new SampleData();
        //TODO assign some generic details
        st.msi.submissionIdentifier = prefix+"-"+outcount;
        st.msi.submissionTitle = "A collection of EMBL-Bank records";
        st.msi.submissionDescription = "These are samples that have corresponding information in EMBL-Bank";

        outcount += 1;
        return st;
        
    }
    
    private void write(SampleData st){
        File outputSubDir = new File(outputFile, st.msi.submissionIdentifier);
        File sampletabPre = new File(outputSubDir, "sampletab.pre.txt");
        outputSubDir.mkdirs();
        
        log.info("Writing "+sampletabPre);
        
        Writer writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(sampletabPre));
            SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
            sampletabwriter.write(st);
            sampletabwriter.close();
        } catch (IOException e) {
            log.error("Unable to write to "+sampletabPre, e);
        } finally {
            if (writer != null){
                try {
                    writer.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
    }
    
    private void parseInput(File inputFile) throws IOException {
        
        List<String> headers = null;

        CSVReader reader = null;
        
        SampleData st = getNewSampleData();

        Pattern latLongPattern = Pattern.compile("([0-9]+\\.?[0-9]*) ([NS]) ([0-9]+\\.?[0-9]*) ([EW])");

        TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
        
        try {
            reader = new CSVReader(new FileReader(inputFile), '\t');
            String [] nextLine;
            int linecount = 0;
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                String accession = null;
                
                //skip the junk left at the end of the file
                if (nextLine[0].trim().equals("SQL> spool off;"))
                    continue;
                
                if (headers == null){
                    headers = new ArrayList<String>();
                    for (String header : nextLine){
                        headers.add(header);
                    }
                } else {
                    if (nextLine.length > headers.size()){
                        log.warn("Line longer than headers "+linecount);
                    }
                    
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
                                        o.setTermSourceREF(st.msi.getOrAddTermSource(ncbitaxonomy));
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
                                    o.setTermSourceREF(st.msi.getOrAddTermSource(ncbitaxonomy));
                                    o.setTermSourceID(value);
                                    s.addAttribute(o);
                                }
                            }
                        }
                    }
                    
                    s.addAttribute(new DatabaseAttribute("EMBL-Bank", s.getNodeName(), "http://www.ebi.ac.uk/ena/data/view/"+s.getNodeName()));
                    
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
                    
                    //if there are too many samples in the file, make a new file
                    
                    if (st.scd.getNodeCount() >= samplecount){
                        write(st);
                        st = getNewSampleData();
                    }
                    
                }
            }
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
            }
        }
        //write what is left to disk
        write(st);
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
