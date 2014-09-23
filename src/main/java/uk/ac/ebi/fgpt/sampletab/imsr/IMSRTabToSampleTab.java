package uk.ac.ebi.fgpt.sampletab.imsr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.Normalizer;

public class IMSRTabToSampleTab {

    private TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public IMSRTabToSampleTab() {
        //Nothing to do in constructor
    }

    public Logger getLog() {
        return log;
    }

    public SampleData convert(String filename) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {
        return convert(new File(filename));
    }

    public SampleData convert(File infile) throws ParseException, IOException, NumberFormatException,
            java.text.ParseException {

        SampleData st = new SampleData();

        String nomenclature = null;
        String strainId = null;
        String stock = null;
        String site = null;
        String state = null;
        String synonym = null;
        String type = null;
        String alleleId = null;
        String alleleSymbol = null;
        String alleleName = null;
        String geneId = null;
        String geneSymbol = null;
        String geneName = null;
        BufferedReader input;
        String line;
        boolean headers = false;

        // these store the data that we need to track , the key in all the maps is the stock
        Map<String, Set<String>> nomencl = new HashMap<String, Set<String>>();
        Map<String, Set<String>> strainid = new HashMap<String, Set<String>>();
        Map<String, Set<String>> synonyms = new HashMap<String, Set<String>>();
        Map<String, Set<String>> types = new HashMap<String, Set<String>>();
        Map<String, Set<String>> states = new HashMap<String, Set<String>>();
        Map<String, Set<List<String>>> mutations = new HashMap<String, Set<List<String>>>();

        log.info("Prepared for reading.");
        try {
            input = new BufferedReader(new FileReader(infile));
            while ((line = input.readLine()) != null) {
                //System.out.println(line);
                // line too short? skip
                if (line.length() == 0) {
                    continue;
                }
                if (!headers) {
                    headers = true;
                    continue;
                }
                String[] entries = line.split("\t");
                
                // Nomenclature Strain/Stock Site(Repository) State Synonyms Type AlleleId AlleleSymbol AlleleName GeneId GeneSymbol GeneName
               nomenclature = entries[0];
               if(nomenclature.isEmpty()){
            	   nomenclature = "";
            	   System.out.println("nomenclature is Empty");
               }
                
               strainId = entries[1];
               
                stock = entries[2];
                if (site == null) {
                    site = entries[3];
                    addSite(st, site);
                }
                state = entries[4];
                if (entries.length > 5){
                    synonym = entries[5];
                } else {
                    synonym = "";
                }
                if (entries.length > 6){
                    type = entries[6];
                } else {
                    type = "";
                }
                if (entries.length > 7){
                    alleleId = entries[7];
                } else {
                    alleleId = "";
                }
                if (entries.length > 8){
                    alleleSymbol = entries[8];
                } else {
                    alleleSymbol = "";
                }
                if (entries.length > 9){
                    alleleName = entries[9];
                } else {
                    alleleName = "";
                }
                if (entries.length > 10){
                    geneId = entries[10];
                } else {
                    geneId = "";
                }
                if (entries.length > 11){
                    geneSymbol = entries[11];
                } else {
                    geneSymbol = "";
                }
                if (entries.length > 12){
                    geneName = entries[12];
                } else {
                    geneName = "";
                }
                
                
                //Storing Nomenclature
                if(!nomenclature.equalsIgnoreCase("?") && !nomencl.containsKey(stock)){
                	nomencl.put(stock, new HashSet<String>());
                }
                
                if (!nomenclature.equalsIgnoreCase("?") && nomenclature.length() > 0 && !nomencl.get(stock).contains(nomenclature)){
                	nomencl.get(stock).add(nomenclature);
                }
                
                //Storing StrainId
                if (!strainid.containsKey(stock)) {
                    strainid.put(stock, new HashSet<String>());
                }
                if (strainId.length() > 0 && !strainid.get(stock).contains(strainId)) {
                    strainid.get(stock).add(strainId);
                }
                
                // always store stock in synonyms to pull them back out later
                if (!synonyms.containsKey(stock)) {
                    synonyms.put(stock, new HashSet<String>());
                }
                if (synonym.length() > 0 && !synonyms.get(stock).contains(synonym)) {
                    synonyms.get(stock).add(synonym);
                }
                
                //Storing types
                if (!types.containsKey(stock)) {
                    types.put(stock, new HashSet<String>());
                }
                if (type.length() > 0 && !types.get(stock).contains(type)) {
                    types.get(stock).add(type);
                }
                
                //Storing states
                if (!states.containsKey(stock)) {
                    states.put(stock, new HashSet<String>());
                }
                if (state.length() > 0 && !states.get(stock).contains(state)) {
                    states.get(stock).add(state);
                }

                //Storing the mutants
                if (geneName.length() > 0) {
                    List<String> mutantlist = new ArrayList<String>();
                    mutantlist.add(alleleId);
                    mutantlist.add(alleleSymbol);
                    mutantlist.add(alleleName);
                    mutantlist.add(geneId);
                    mutantlist.add(geneSymbol);
                    mutantlist.add(geneName);

                    // check the stock name is in the mutations map
                    if (!mutations.containsKey(stock)) {
                        mutations.put(stock, new HashSet<List<String>>());
                    }
                    // actually add the mutationlist
                    if (!mutations.get(stock).contains(mutantlist)) {
                        mutations.get(stock).add(mutantlist);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            log.error("Unable to find tab file", e);
        }
        getLog().info("Finished reading, starting conversion");

        // now all the data has been parsed into memory, but we need to turn
        // them into sample objects

        for (String name : synonyms.keySet()) {
            SampleNode newnode = new SampleNode();
            newnode.setNodeName(name);

            if (synonyms.containsKey(name)) {
                for (String thissynonym : synonyms.get(name)) {
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", thissynonym);
                    // insert all synonyms at position zero so they display next
                    // to name
                    newnode.addAttribute(synonymattrib, 0);
                }
            }
            
            //Nomenclature as comment
            if(nomencl.containsKey(name)){
            	for (String thisnomenclature :nomencl.get(name)){
            		CommentAttribute nomenclattrib = new CommentAttribute("Nomenclature", thisnomenclature);
            		newnode.addAttribute(nomenclattrib);
            	}
            }

            //Strain Id 
            if(strainid.containsKey(name)){
            	for(String thisstrainId :strainid.get(name)){
            		CharacteristicAttribute strainIdattrib = new CharacteristicAttribute("strain", thisstrainId);
            		newnode.addAttribute(strainIdattrib);
            	}
            }
            
            if (states.containsKey(name) && (states.get(name).size() > 1)) {
                // if there are multiple materials
                // create a strain sample and derive individual materials

                MaterialAttribute matterialattribute = new MaterialAttribute("strain");
                newnode.addAttribute(matterialattribute);

                // add the strain node to the st
                st.scd.addNode(newnode);

                for (String material : states.get(name)) {
                    SampleNode materialnode = new SampleNode();
                    materialnode.setNodeName(name + " " + material);

                    matterialattribute = new MaterialAttribute(material);
                    materialnode.addAttribute(matterialattribute);

                    newnode.addChildNode(materialnode);
                    materialnode.addParentNode(newnode);
                    
                    st.scd.addNode(materialnode);

                }

            } else if (states.containsKey(name) && (states.get(name).size() == 1)) {
                // if there is only one material
                // this is a for loop that only runs once because no easy way to access a member of a set
                for (String material : states.get(name)) {
                    newnode.addAttribute(new MaterialAttribute(material));
                    break;
                }

                // TODO add efo mappings

                // add the node to the st
                st.scd.addNode(newnode);
            } else {
                // no material
                // should never happen?
                // TODO check
                log.warn("found a sample without material: " + name);
                continue;
            }

            // all IMSR samples must be mice.
            OrganismAttribute organismattribute = new OrganismAttribute();
            organismattribute.setAttributeValue("Mus musculus");
            organismattribute.setTermSourceREF(st.msi.getOrAddTermSource(ncbitaxonomy));
            organismattribute.setTermSourceIDInteger(10090);
            newnode.addAttribute(organismattribute);

            //add the mutations as characteristics
            if (mutations.containsKey(name)) {
                for (List<String> thismutation : mutations.get(name)) {
                	alleleId = thismutation.get(0);
                    alleleSymbol = thismutation.get(1);
                    alleleName = thismutation.get(2);
                    geneId = thismutation.get(3);
                    geneSymbol = thismutation.get(4);
                    geneName = thismutation.get(5);
                    if (alleleId.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Allele ID", alleleId));
                    }
                    if (alleleSymbol.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Allele Symbol", alleleSymbol));
                    }
                    if (alleleName.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Allele Name", alleleName));
                    }
                    if (geneId.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Gene ID", geneId));
                    }
                    if (geneSymbol.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Gene Symbol", geneSymbol));
                    }
                    if (geneName.length() > 0) {
                        newnode.addAttribute(new CharacteristicAttribute("Gene Name", geneName));
                    }
                }
            }
            
            //add per strain links to IMSR
            String url = "http://www.findmice.org/summary?query=\""+URLEncoder.encode(name, "UTF-8")+"\"&repositories="+URLEncoder.encode(site, "UTF-8");
            DatabaseAttribute dbattr = new DatabaseAttribute("IMSR", name, url);
            newnode.addAttribute(dbattr);
        }

        //create a group for nodes to go into
        GroupNode othergroup = new GroupNode("Other Group");
        st.scd.addNode(othergroup);
        
        //add nodes to that group
        for (SampleNode sample : st.scd.getNodes(SampleNode.class)) {
            log.debug("Adding sample " + sample.getNodeName() + " to group " + othergroup.getNodeName());
            othergroup.addSample(sample);
        }
        
        
        
        
        getLog().info("Finished convert()");
        
        Normalizer norm = new Normalizer();
        norm.normalize(st);
        
        return st;
    }

    public void convert(File file, Writer writer) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {
        getLog().debug("recieved magetab, preparing to convert");
        SampleData st = convert(file);

        getLog().info("SampleTab converted, preparing to write");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        sampletabwriter.write(st);
        getLog().info("SampleTab written");
        sampletabwriter.close();

    }

    public void convert(File infile, String outfilename) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {

        convert(infile, new File(outfilename));
    }

    public void convert(File infile, File outfile) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {

        // create parent directories, if they dont exist
        outfile = outfile.getAbsoluteFile();
        if (outfile.isDirectory()) {
            outfile = new File(outfile, "sampletab.pre.txt");
        }

        if (!outfile.getParentFile().exists()) {
            outfile.getParentFile().mkdirs();
        }

        convert(infile, new FileWriter(outfile));
    }

    public void convert(String infilename, Writer writer) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {
        convert(new File(infilename), writer);
    }

    public void convert(String infilename, File outfile) throws IOException, ParseException, NumberFormatException,
            java.text.ParseException {
        convert(infilename, new FileWriter(outfile));
    }

    public void convert(String infilename, String outfilename) throws IOException, ParseException,
            NumberFormatException, java.text.ParseException {
        convert(infilename, new File(outfilename));
    }

    private void addSite(SampleData st, String site) throws NumberFormatException, java.text.ParseException,
            IOException {
        log.info("Adding site " + site);
        
        IMSRTabWebSummary summary = IMSRTabWebSummary.getInstance();

        if (!summary.sites.contains(site)) throw new IllegalArgumentException(""+site+" not found in sites summary");
        
        int index = summary.sites.indexOf(site);
        
        if (summary.facilities.size() <= index) throw new IllegalArgumentException(""+site+" not found in facilities summary");
        if (summary.strainss.size() <= index) throw new IllegalArgumentException(""+site+" not found in strainss summary");
        if (summary.esliness.size() <= index) throw new IllegalArgumentException(""+site+" not found in esliness summary");
        if (summary.totals.size() <= index) throw new IllegalArgumentException(""+site+" not found in sites summary");
        if (summary.updates.size() <= index) throw new IllegalArgumentException(""+site+" not found in sites summary");
        
        st.msi.submissionTitle = "International Mouse Strain Resource - " + summary.facilities.get(index);
        st.msi.submissionDescription = "The IMSR is a searchable online database of mouse strains and stocks available worldwide, including inbred, mutant, and genetically engineered mice. The goal of the IMSR is to assist the international scientific community in locating and obtaining mouse resources for research. These samples are held by "
                + summary.facilities.get(index);
        st.msi.submissionReleaseDate = new GregorianCalendar(2011, 10, 10).getTime();
        st.msi.submissionUpdateDate = summary.updates.get(index);
        st.msi.submissionIdentifier = "GMS-" + site;
        st.msi.submissionReferenceLayer = true;

        st.msi.organizations.add(new Organization("International Mouse Strain Resource", null, "http://www.findmice.org/", null, null));
        st.msi.organizations.add(new Organization(summary.facilities.get(index), null, "http://www.findmice.org/", null, "Biomaterial Provider"));
        
        st.msi.databases.add(new Database("IMSR", "http://www.findmice.org/summary?query=&repositories="+site, site));
    }

    public static void main(String[] args) {

        IMSRTabToSampleTab converter = new IMSRTabToSampleTab();
        converter.doMain(args);
    }

    public void doMain(String[] args){
        if (args.length < 2) {
            System.out.println("Must provide an IMSR Tab input filename and a SampleTab output filename.");
            return;
        }
        String imsrTabFilename = args[0];
        String sampleTabFilename = args[1];
        
        try {
            convert(imsrTabFilename, sampleTabFilename);
        } catch (ParseException e) {
            log.error("Error converting " + imsrTabFilename + " to " + sampleTabFilename, e);
            System.exit(2);
            return;
        } catch (IOException e) {
            log.error("Error converting " + imsrTabFilename + " to " + sampleTabFilename, e);
            System.exit(3);
            return;
        } catch (NumberFormatException e) {
            log.error("Error converting " + imsrTabFilename + " to " + sampleTabFilename, e);
            System.exit(4);
            return;
        } catch (java.text.ParseException e) {
            log.error("Error converting " + imsrTabFilename + " to " + sampleTabFilename, e);
            System.exit(5);
            return;
        }
    }
}
