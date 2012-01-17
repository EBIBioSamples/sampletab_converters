package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.PRIDEutils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class PRIDEXMLToSampleTab {

    // singlton instance
    private static final PRIDEXMLToSampleTab instance = new PRIDEXMLToSampleTab();

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
        
    private PRIDEXMLToSampleTab() {
        // private constructor to prevent accidental multiple initialisations

    }

    public static PRIDEXMLToSampleTab getInstance() {
        return instance;
    }

    public Logger getLog() {
        return log;
    }


    public SampleData convert(Set<File> infiles) throws DocumentException {
        
        SampleData st = new SampleData();
        
        for (File infile : infiles){
            Element expcollection = XMLUtils.getDocument(infile).getRootElement();
            Element exp = XMLUtils.getChildByName(expcollection, "Experiment");
            Element additional = XMLUtils.getChildByName(exp, "additional");
            Element description = XMLUtils.getChildByName(exp, "description");
            Element admin = XMLUtils.getChildByName(description, "admin");
            Element sampledescription = XMLUtils.getChildByName(admin, "sampleDescription");
            String accession = XMLUtils.getChildByName(exp, "ExperimentAccession").getTextTrim();
            
            if (st.msi.submissionTitle == null)
                st.msi.submissionTitle = XMLUtils.getChildByName(exp, "Title").getTextTrim();
            //PRIDE dont have submission description
            //actually maybe it does as a CVparam...
            st.msi.submissionReferenceLayer = false;
            
            
            for (Element contact : XMLUtils.getChildrenByName(admin, "contact")){
                String name = XMLUtils.getChildByName(exp, "name").getTextTrim();
                String institution = XMLUtils.getChildByName(exp, "institution").getTextTrim();
                if (!st.msi.organizationName.contains(institution))
                    st.msi.organizationName.add(institution);
                String[] splitnames = PRIDEutils.splitName(name);
                //TODO check does not already exist?
                st.msi.personFirstName.add(splitnames[0]);
                st.msi.personInitials.add(splitnames[1]);
                st.msi.personLastName.add(splitnames[2]);
                st.msi.personEmail.add("");
                st.msi.personRole.add("Submitter");
            }
            
            for (Element reference : XMLUtils.getChildrenByName(exp, "Reference")){
                for (Element referenceadditional : XMLUtils.getChildrenByName(reference, "additional")){
                    for (Element referenceaddpar : XMLUtils.getChildrenByName(referenceadditional, "cvParam")){
                        if (referenceaddpar.attributeValue("cvLabel").trim().equals("PubMed")){
                            //some PubMed IDs have full URLs, strip them
                            String pubmedid = referenceaddpar.attributeValue("accession").trim();
                            pubmedid.replace("http://www.ncbi.nlm.nih.gov/pubmed/", "");
                            //check to avoid duplicates
                            if (!st.msi.publicationPubMedID.contains(pubmedid)){
                                st.msi.publicationPubMedID.add(pubmedid);
                                st.msi.publicationDOI.add("");
                            }
                        }
                    }
                }
            }
            
            SampleNode sample = new SampleNode();
            sample.setNodeName("GPR-"+accession);
            
            DatabaseAttribute dbattr = new DatabaseAttribute("PRIDE", accession, "http://www.ebi.ac.uk/pride/showExperiment.do?value="+accession);
            sample.addAttribute(dbattr);

            for (Element cvparam : XMLUtils.getChildrenByName(sampledescription, "cvParam")){
                String name = cvparam.attributeValue("name").trim();
                String value = cvparam.attributeValue("value").trim();
                if (value == null){
                    //some PRIDE attributes are boolean
                    //set their value to be their name
                    value = name;
                }
                //TODO  use special attribute classes where appropriate
                CharacteristicAttribute attr = new CharacteristicAttribute(name, value);
                if (cvparam.attributeValue("cvLabel") != null && cvparam.attributeValue("accession") != null){
                    attr.termSourceREF = cvparam.attributeValue("cvLabel").trim();
                    //TODO make sure that this term source is then added to msi section
                    attr.termSourceID = cvparam.attributeValue("accession").trim();
                }
                sample.addAttribute(attr);
            }
            
            for (Element cvparam : XMLUtils.getChildrenByName(additional, "cvParam")){
                String name = cvparam.attributeValue("name").trim();
                String value = cvparam.attributeValue("value").trim();
                if (value == null){
                    //some PRIDE attributes are boolean
                    //set their value to be their name
                    value = name;
                }
                CharacteristicAttribute attr = new CharacteristicAttribute(name, value);
                if (cvparam.attributeValue("cvLabel") != null && cvparam.attributeValue("accession") != null){
                    attr.termSourceREF = cvparam.attributeValue("cvLabel").trim();
                    //TODO make sure that this term source is then added to msi section
                    attr.termSourceID = cvparam.attributeValue("accession").trim();
                }
                sample.addAttribute(attr);
            }
            
            for (Element userparam : XMLUtils.getChildrenByName(additional, "userParam")){
                String name = userparam.attributeValue("name").trim();
                String value = userparam.attributeValue("value").trim();
                if (value == null){
                    //some PRIDE attributes are boolean
                    //set their value to be their name
                    value = name;
                }
                CommentAttribute attr = new CommentAttribute(name, value);
                sample.addAttribute(attr);
            }
            
            try {
                st.scd.addNode(sample);
            } catch (ParseException e) {
                log.error("Unable to add node "+sample);
                e.printStackTrace();
                continue;
            }
            
            
            
        }
        //these can only be calculated after all other steps
        
        //submission id is the minimum sample id
        List<String> submitids = new ArrayList<String>();
        for (SCDNode in : st.scd.getAllNodes()){
            submitids.add(in.getNodeName());
        }
        Collections.sort(submitids);
        st.msi.submissionIdentifier = submitids.get(0) ;
        
        getLog().info("Finished convert()");
        return st;
    }

    public void convert(Set<File> infiles, Writer writer) throws IOException, DocumentException  {
        getLog().debug("recieved infiles, preparing to convert");
        SampleData st = convert(infiles);

        getLog().info("SampleTab converted, preparing to write");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        sampletabwriter.write(st);
        getLog().info("SampleTab written");
        sampletabwriter.close();

    }

    public void convert(Set<File> infiles, File sampletabFile) throws IOException, DocumentException {

        // create parent directories, if they dont exist
        sampletabFile = sampletabFile.getAbsoluteFile();
        if (sampletabFile.isDirectory()) {
            sampletabFile = new File(sampletabFile, "sampletab.pre.txt");
        }
        if (!sampletabFile.getParentFile().exists()) {
            sampletabFile.getParentFile().mkdirs();
        }

        convert(infiles, new FileWriter(sampletabFile));
    }

    public void convert(Set<File> infiles, String outfilename) throws IOException, DocumentException  {

        convert(infiles, new File(outfilename));
    }


    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Must provide a list of at least one PRIDE XML files and a SampleTab output filename.");
            return;
        }
        
        Set<String> pridefilenames = new HashSet<String>();
        for (int i = 0; i < args.length-1; i++){
            pridefilenames.add(args[i]);
        }
        
        Set<File> pridefiles = new HashSet<File>();
        for (String pridefilename : pridefilenames){
            pridefiles.add(new File(pridefilename));
        }
        
        String sampleTabFilename = args[args.length-1];

        PRIDEXMLToSampleTab converter = PRIDEXMLToSampleTab.getInstance();

        try {
            converter.convert(pridefiles, sampleTabFilename);
        } catch (IOException e) {
            System.out.println("Error converting " + pridefilenames + " to " + sampleTabFilename);
            e.printStackTrace();
            return;
        } catch (DocumentException e) {
            System.out.println("Error converting " + pridefilenames + " to " + sampleTabFilename);
            e.printStackTrace();
            return;
        }
    }
}
