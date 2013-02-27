package uk.ac.ebi.fgpt.sampletab.emblbank;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonException;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonUtils;

public class EMBLBankRunnable implements Runnable {



    private TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    Pattern latLongPattern = Pattern.compile("([0-9]+\\.?[0-9]*) ([NS]) ([0-9]+\\.?[0-9]*) ([EW])");
    
    private final List<String[]> lines;
    
    private final Map<String, Set<String>> groupMap;
    
    private final String groupID;
    
    private final File outputDir;
    
    private final String prefix;
    
    private final boolean wgs;
    private final boolean tsa;
    private final boolean bar;
    private final boolean cds;


    private Logger log = LoggerFactory.getLogger(getClass());
    
    public EMBLBankRunnable(List<String[]> lines, 
            Map<String, Set<String>> groupMap,
            String groupID,
            File outputDir, String prefix, boolean wgs, boolean tsa, boolean bar, boolean cds) {
        this.lines = lines;
        this.groupMap = groupMap;
        this.groupID = groupID;
        this.outputDir = outputDir;
        this.prefix = prefix;
        this.wgs = wgs;
        this.tsa = tsa;
        this.bar = bar;
        this.cds = cds;
        
    }
    
    private SampleNode lineToSample(String[] line, EMBLBankHeaders headers){

        SampleNode s = new SampleNode();
        
        for (int i = 0; i < line.length && i < headers.size(); i++){
            String header = headers.get(i).trim();
            String value = line[i].trim();
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
                    //fails at these values
                    // 19.02 N 72.46
                    // 19.02 N 72.
                    
                    if (m.lookingAt()){
                        Float latitude = new Float(m.group(1));  
                        if (m.group(2).equals("S")){
                            latitude = -latitude;
                        }
                        UnitAttribute decimaldegrees = new UnitAttribute();
                        decimaldegrees.type = "unit";
                        decimaldegrees.setAttributeValue("decimal degree");
                        //TODO ontology term
                        
                        CharacteristicAttribute lat =new CharacteristicAttribute("Latitude", latitude.toString());
                        lat.unit = decimaldegrees;
                        s.addAttribute(lat);
                        
                        Float longitude = new Float(m.group(3));  
                        if (m.group(4).equals("W")){
                            longitude = -longitude;
                        }
                        CharacteristicAttribute longit = new CharacteristicAttribute("Longitude", longitude.toString());
                        longit.unit = decimaldegrees;
                        s.addAttribute(longit);
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
                            o = new OrganismAttribute(TaxonUtils.getSpeciesOfID(taxID));
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
        String dbname = "EMBL-Bank";
        if (wgs){
            dbname += " (WGS)";
        } else if (tsa){
            dbname += " (TSA)";
        } else if (bar){
            dbname += " (Barcode)";
        } else if (cds){
            dbname += " (CDS)";
        }
        s.addAttribute(new DatabaseAttribute(dbname, s.getNodeName(), "http://www.ebi.ac.uk/ena/data/view/"+s.getNodeName()));
        
        return s;
    }
    
    public Set<Publication> getPublications(String[] line, EMBLBankHeaders headers){
        Set<Publication> pubSet = new HashSet<Publication>();
        
        String[] doiStrings = new String[0];
        if (headers.doiindex > 0 && headers.doiindex < line.length){
            doiStrings = line[headers.doiindex].trim().split(",");
        }
        
        String[] pubmedStrings = new String[0];
        if (headers.pubmedindex > 0 && headers.pubmedindex < line.length){
            pubmedStrings = line[headers.pubmedindex].trim().split(",");
        }
        
        int maxlength = doiStrings.length;
        if (pubmedStrings.length > maxlength){
            maxlength = pubmedStrings.length;
        }

        for (int i = 0; i < maxlength; i++){
            String pubmed = null;
            String doi = null;
            
            if (i < pubmedStrings.length){
                pubmed = pubmedStrings[i].trim();
            }
            
            if (i < doiStrings.length){
                doi = doiStrings[i].trim();
            }
            
            if ((pubmed != null && pubmed.length() > 0)
                    || (doi != null && doi.length() > 0)){
                //log.debug("Publication "+pubmed+" "+doi);
                Publication p = new Publication(pubmed, doi);
                pubSet.add(p);
            }
        }
        
        return pubSet;
    }

    public void run() {
        
        
        //get a group to process
        //loop over the inputFile
        //if you find a line that matches it, then process it
        //add that sample to the sampletab file
        //output sampletab file

        SampleData st = new SampleData();
        st.msi.termSources.add(ncbitaxonomy);
        Set<String> groupIDs = groupMap.get(groupID);
        
        
        EMBLBankHeaders headers = null;
        
        for (String[] nextLine : lines) {
            if (headers == null){
                try {
                    headers = new EMBLBankHeaders(nextLine);
                } catch (IOException e) {
                    log.error("Problem using headers", e);
                    return;
                }
            } else {
            
                String accession = nextLine[0].trim();
                log.debug("First processing "+accession);
                if (groupIDs.contains(accession)) {

                    SampleNode s = lineToSample(nextLine, headers);

                    try {
                        if (s.getNodeName() == null) {
                            log.error("Unable to add node with null name");
                        } else if (st.scd.getNode(s.getNodeName(), SampleNode.class) != null) {
                            //this node name has already been used
                            log.error("Unable to add duplicate node with name "+s.getNodeName());
                        } else if (s.getAttributes().size() <= 1) {
                            //dont add uninformative samples
                            //will always have one database attribute
                            log.warn("Refusing to add sample "+s.getNodeName()+" without attributes");
                        } else {
                            st.scd.addNode(s);
                        }
                    } catch (ParseException e) {
                        log.error("Unable to add node "+s.getNodeName(), e);
                    }     
                    
                    //add publications
                    for (Publication publication : getPublications(nextLine, headers)) {
                        if (publication != null && 
                                !st.msi.publications.contains(publication)) {
                            st.msi.publications.add(publication);
                        }
                    }
                }
            }
        }        

        //create some information
        st.msi.submissionIdentifier = prefix+"-"+groupID;
        st.msi.submissionIdentifier = st.msi.submissionIdentifier.replace(" ", "-");
        st.msi.submissionIdentifier = st.msi.submissionIdentifier.replace("&", "and");
        if (wgs) {
            //its a whole genome shotgun sample, describe as such
            String speciesName = null;
            for (SampleNode sn : st.scd.getNodes(SampleNode.class)){
                for (SCDNodeAttribute a : sn.getAttributes()){
                    if (OrganismAttribute.class.isInstance(a)){
                        speciesName = a.getAttributeValue();
                    }
                }
            }
            if (speciesName == null){
                log.warn("Unable to determine species name");
            } else {
                st.msi.submissionTitle = "Whole Genome Shotgun sequencing of "+speciesName;
            }
        } else if (tsa){
            //its a transcriptome shotgun sample, describe as such
            String speciesName = null;
            for (SampleNode sn : st.scd.getNodes(SampleNode.class)){
                for (SCDNodeAttribute a : sn.getAttributes()){
                    if (OrganismAttribute.class.isInstance(a)){
                        speciesName = a.getAttributeValue();
                    }
                }
            }
            if (speciesName == null){
                log.warn("Unable to determine species name");
            } else {
                st.msi.submissionTitle = "Transcriptome Shotgun sequencing of "+speciesName;
            }
        } else if (bar){
            //need to generate a title for the submission
            st.msi.submissionTitle = SampleTabUtils.generateSubmissionTitle(st);
            //TODO use the title of a publication, if one exists
        } else if (cds){
            //need to generate a title for the submission
            st.msi.submissionTitle = SampleTabUtils.generateSubmissionTitle(st);
        } else {
            log.warn("No submission type indicated");
            
        }
        
        
        log.debug("No. of publications = "+st.msi.publications.size());
        log.debug("Empty? "+st.msi.publications.isEmpty());
        
        
        

        //add an intermediate subdir layer based on the initial 7 characters (GEM-...)
        File outputSubDir = new File(outputDir, SampleTabUtils.getSubmissionDirPath(st.msi.submissionIdentifier));
        outputSubDir.mkdirs();
        File sampletabPre = new File(outputSubDir, "sampletab.pre.txt");
        
        uk.ac.ebi.fgpt.sampletab.Normalizer norm = new uk.ac.ebi.fgpt.sampletab.Normalizer();
        norm.normalize(st);
        
        log.info("Writing "+sampletabPre);
        synchronized(SampleTabWriter.class){
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
        }
        //remove this group from the groupmap
        groupMap.remove(groupID);
        
    }
}
