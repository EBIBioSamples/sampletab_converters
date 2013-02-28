package uk.ac.ebi.fgpt.sampletab.other;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class Cosmic extends AbstractDriver {


    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filename")
    protected String inputFilename;

    @Argument(required=true, index=1, metaVar="OUTPUT", usage = "output dir")
    private String outputDirName;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new Cosmic().doMain(args);
    }    
    
    
    private Map<String, List<String[]>> getGrouping(File inputFile){

        Map<String, List<String[]>> grouping = new HashMap<String, List<String[]>>();
        
        
        CSVReader reader = null;
        int linecount = 0;
        String [] nextLine = null;
        String [] headers = null;
        try {
            reader = new CSVReader(new FileReader(inputFile), "\t".charAt(0));
            while ((nextLine = reader.readNext()) != null) {
                linecount += 1;
                if (linecount % 10000 == 0){
                    log.info("processing line "+linecount);
                }
                                
                if (headers == null || linecount == 0) {
                    headers = nextLine;
                } else {
                    //use pubmedid as group id
                    String groupID = nextLine[21].trim();
                    if (groupID.equals("")){
                        //some don't have a pubmedid, but do have a TCGA pattern sample name
                        if (nextLine[3].startsWith("TCGA-")){
                            groupID = "TCGA";
                        } else {
                            //some don't have either a pubmed id or a tcga pattern
                            //in these cases just stuff then in another group
                            groupID = "other";
                        }
                    } else if (!groupID.matches("[0-9]+")){
                        log.error("Strange pubmedid "+groupID);
                    }
                    
                    if (!grouping.containsKey(groupID)){
                        grouping.put(groupID, new LinkedList<String[]>());
                        grouping.get(groupID).add(headers);
                    }
                    grouping.get(groupID).add(nextLine);
                }
            }

            reader.close();
        } catch (FileNotFoundException e) {
            log.error("Error processing "+inputFile, e);
        } catch (IOException e) {
            log.error("Error processing "+inputFile, e);
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
            }
        }
        
        return grouping;
    }
    
    protected void doMain(String[] args) {
        super.doMain(args);
        
        File inputFile = new File(inputFilename);
        if (!inputFile.exists()){
            log.error("File "+inputFile+" does not exist!");
            return;
        }
        
        Map<String, List<String[]>> grouping = getGrouping(inputFile);
        
        for (String groupid : grouping.keySet()) {
            String[] headers = null;
            
            SampleData st = new SampleData();
            st.msi.submissionTitle = "COSMIC - Catalogue Of Somatic Mutations In Cancer";
            st.msi.submissionDescription = "All cancers arise as a result of the acquisition of a series of fixed DNA sequence abnormalities, mutations, many of which ultimately confer a growth advantage upon the cells in which they have occurred. There is a vast amount of information available in the published scientific literature about these changes. COSMIC is designed to store and display somatic mutation information and related details and contains information relating to human cancers.";
            //TODO add sanger organization
            TermSource efo = new TermSource("EFO", "http://www.ebi.ac.uk/efo/", "2.23");
            st.msi.termSources.add(efo);
            TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy", null);
            st.msi.termSources.add(ncbitaxonomy);
            
            //TODO decide if to put cosmic on its own, or roll it into GSB
            st.msi.submissionIdentifier = "GCM-"+groupid;
            
            //mark as reference layer
            st.msi.submissionReferenceLayer = true;
            
            //add publication if groupid is pubmed id
            if (groupid.matches("[0-9]+")){
                st.msi.publications.add(new Publication(groupid, null));
            }
            
            for (String[] nextLine : grouping.get(groupid)) {
                if (headers == null) {
                    headers = nextLine;
                } else {
                    Map<String, String> line = new HashMap<String, String>();
                    for (int i = 0; i < nextLine.length; i++) {
                        line.put(headers[i], nextLine[i]);
                    }
                    String sampleName = nextLine[3];
                    String sampleID = nextLine[4];
                    
                    
                    SampleNode sample = st.scd.getNode(sampleID, SampleNode.class);
                    if (sample == null) {
                        sample = new SampleNode(sampleID);
                        try {
                            st.scd.addNode(sample);
                        } catch (ParseException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }

                    sample.addAttribute(new CommentAttribute("synonym", sampleName));
                    sample.addAttribute(new CommentAttribute("ID tumor", line.get("ID_tumor")));
                    sample.addAttribute(new OrganismAttribute("Homo sapiens", ncbitaxonomy, 9606));
                    
                    
                    if (!line.get("Primary site").equals("NS")) {
                        String organismPart = line.get("Primary site");
                        if (!line.get("Site subtype").equals("NS")) {
                            organismPart = organismPart+" : "+line.get("Site subtype");
                        }
                        sample.addAttribute(new CharacteristicAttribute("organism part", organismPart));
                    }
                    
                    if (!line.get("Primary histology").equals("NS")) {
                        String diseaseState = line.get("Primary histology");
                        if (!line.get("Histology subtype").equals("NS")) {
                            diseaseState = diseaseState+" : "+line.get("Primary histology");
                        }
                        sample.addAttribute(new CharacteristicAttribute("disease state", diseaseState));
                    }
                    
                    if (!line.get("Sample source").equals("NS")) {
                        sample.addAttribute(new MaterialAttribute(line.get("Sample source")));
                    }
                    
                    if (!line.get("Tumour origin").equals("NS")) {
                        sample.addAttribute(new CharacteristicAttribute("tumour origin", line.get("Tumour origin")));
                    }
                    
                    for (String part : line.get("Comments").split(",")){
                        part = part.trim();
                        String[] splitDash = part.split(" - ");
                        String[] splitColon = part.split(":");
                        if (part.length() == 0){
                            //do nothing
                        } else if (part.contains(":") && splitColon.length == 2){
                            String key = splitColon[0].trim();
                            String value = splitColon[1].trim();
                            sample.addAttribute(new CommentAttribute(key, value));
                        } else if (part.contains(" - ") && splitDash.length == 2){
                            String key = splitDash[0].trim();
                            String value = splitDash[1].trim();
                            sample.addAttribute(new CommentAttribute(key, value));
                        } else {
                            log.warn("Unrecognized comment part "+part);
                            sample.addAttribute(new CommentAttribute("other", part));
                        }
                    }
                    
                    sample.addAttribute(new DatabaseAttribute("COSMIC", sample.getNodeName(), "http://cancer.sanger.ac.uk/cosmic/sample/overview?name="+sample.getNodeName()));
                }
            }
            
            //write output
            SampleTabWriter writer = null ; 
            File outputFile = new File(outputDirName, st.msi.submissionIdentifier);
            outputFile.mkdirs();
            outputFile = new File(outputFile, "sampletab.pre.txt");
            try {
                writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outputFile)));
                writer.write(st);
                writer.close();
            } catch (IOException e) {
                log.error("Error writing to "+outputFile, e);
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
    }

}
