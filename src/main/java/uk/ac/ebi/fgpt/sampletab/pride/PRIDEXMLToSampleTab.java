package uk.ac.ebi.fgpt.sampletab.pride;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Set;

import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.PRIDEutils;
import uk.ac.ebi.fgpt.sampletab.utils.TermSourceUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class PRIDEXMLToSampleTab {

    private TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
        
    public PRIDEXMLToSampleTab() {
        
    }
        
    public String getOrAddTermSource(SampleData st, String key) {

        if (st == null){
            throw new IllegalArgumentException("SampleData object must not be null");
        }
        
        TermSource termSource = new TermSource(key, null, null);
        termSource = TermSourceUtils.guessURL(termSource);
        if (termSource == null) {
            //throw new IllegalArgumentException("key not recognized ("+key+")");
            log.warn("term source not recognized ("+key+")");
            return null;
        }
        
        return st.msi.getOrAddTermSource(termSource);
    }    

    private SCDNodeAttribute cvParamToAttribute(SampleData st, Element cvparam) {
        return  cvParamToAttribute(st, cvparam, null);
    }

    private SCDNodeAttribute cvParamToAttribute(SampleData st, Element cvparam, Integer subsampleid) {

        String name = cvparam.attributeValue("name").trim();
        String value = cvparam.attributeValue("value");
        if (value != null){
            value = value.trim();
        }
        
        if (subsampleid != null){
            if (value != null && value.equals("SUBSAMPLE_"+subsampleid)){
                //put the value as the name, but then no other name?
                value = name;
            } else if (value != null && value.startsWith("SUBSAMPLE_")){
                //specific to a diffferent sub-sample, skip
                return null;
            } else {
                //not a sub-sample specific element, continue
            }
        }
        
        String cvLabel = null;
        if (cvparam.attributeValue("cvLabel") != null) {
            cvLabel = cvparam.attributeValue("cvLabel").trim();
            if (cvLabel.length() == 0){
                cvLabel = null;
            }
        }
        
        String cvAccession = null;
        if (cvparam.attributeValue("accession") != null) {
            cvAccession = cvparam.attributeValue("accession").trim();
            if (cvAccession.length() == 0){
                cvAccession = null;
            }
        }
        
        if (value == null || value.length() == 0) {
            //some PRIDE attributes have neither name nor value! e.g. 8695
            if (name == null || name.length() == 0) {
                return null;
            }
            //some PRIDE attributes are boolean
            //set their value to be their name
            value = name;
        }
        
        SCDNodeAttribute attr = null;
        //TODO use special attribute classes where appropriate
        if ("NEWT".equals(cvLabel)) {
            try {
                Integer termSourceID = new Integer(cvAccession);
                String termSourceREF = st.msi.getOrAddTermSource(ncbitaxonomy);
                attr = new OrganismAttribute(name, termSourceREF, termSourceID);
            } catch (NumberFormatException e){
                attr = new OrganismAttribute(name);
            }
        } else {
            CharacteristicAttribute charac = new CharacteristicAttribute(name, value);
            if (cvLabel != null 
                    && cvLabel.length() > 0 
                    && cvAccession != null 
                    && cvAccession.length() > 0){
                cvLabel = getOrAddTermSource(st, cvLabel);
                if (cvLabel != null) {
                    charac.setTermSourceREF(cvLabel);
                    charac.setTermSourceID(cvAccession);
                }
            }
            attr = charac;
        }
        return attr;
    }

    private SCDNodeAttribute userParamToAttribute(SampleData st, Element userparam){
        return  userParamToAttribute(st, userparam, null);
    }
    private SCDNodeAttribute userParamToAttribute(SampleData st, Element userparam, Integer subsampleid){
        String name = userparam.attributeValue("name").trim();
        String value = userparam.attributeValue("value");
        if (value != null){
            value = value.trim();
        }
        
        if (subsampleid != null){
            if (name.equals("SUBSAMPLE_"+subsampleid)){
                //put the name as the value, but then no other value?
                name = value;
            } else if (name.startsWith("SUBSAMPLE_")){
                //specific to a diffferent sub-sample, skip
                return null;
            } else {
                if (value != null && value.equals("SUBSAMPLE_"+subsampleid)){
                    //put the value as the name, but then no other name?
                    value = name;
                } else if (value != null && value.startsWith("SUBSAMPLE_")){
                    //specific to a diffferent sub-sample, skip
                    return null;
                } else {
                    //not a sub-sample specific element, continue
                }
            }
        }
        
        if (value == null){
            //some PRIDE attributes are boolean
            //set their value to be "unknown"
            value = name;
            name = "unknown";
        } else {
            value = value.trim();
        }
        CommentAttribute attr = new CommentAttribute(name, value);
        return attr;
    }
    
    private boolean isSubSample(Element sampledescription){
        for (Element userparam : XMLUtils.getChildrenByName(sampledescription, "userParam")){
            String name = userparam.attributeValue("name").trim();
            if (name.startsWith("SUBSAMPLE_")){
                return true;
            }
        }
        return false;
    }
    
    private boolean hasSubSample(Element sampledescription, int subsampleid){
        for (Element userparam : XMLUtils.getChildrenByName(sampledescription, "userParam")){
            String name = userparam.attributeValue("name").trim();
            if (name.startsWith("SUBSAMPLE_"+subsampleid)){
                return true;
            }
        }
        return false;
    }
    
    
    public SampleData convert(Set<File> infiles, String submissionId) throws DocumentException, FileNotFoundException {
        
        SampleData st = new SampleData();
        st.msi.submissionReferenceLayer = false;
        //PRIDE does not track dates :(
        st.msi.submissionReleaseDate = new Date();
        st.msi.submissionUpdateDate = new Date();

        st.msi.submissionIdentifier = submissionId;
        st.msi.databases.add(new Database("PRIDE", submissionId.substring(4), 
                "https://www.ebi.ac.uk/pride/archive/projects/"+submissionId.substring(4)));
        
        
        for (File infile : infiles) {
            //ensure using absolute filenames
            infile = infile.getAbsoluteFile();
            
            Element expcollection = XMLUtils.getDocument(infile).getRootElement();
            Element exp = XMLUtils.getChildByName(expcollection, "Experiment");
            Element additional = XMLUtils.getChildByName(exp, "additional");
            Element mzd = XMLUtils.getChildByName(exp, "mzData");
            Element description = XMLUtils.getChildByName(mzd, "description");
            Element admin = XMLUtils.getChildByName(description, "admin");
            Element sampledescription = XMLUtils.getChildByName(admin, "sampleDescription");
            Element samplename = XMLUtils.getChildByName(admin, "sampleName");
            String accession = XMLUtils.getChildByName(exp, "ExperimentAccession").getTextTrim();
            
            if (st.msi.submissionTitle == null || st.msi.submissionTitle.length() == 0) {
                st.msi.submissionTitle = XMLUtils.getChildByName(exp, "Title").getTextTrim();
            }
            
            //PRIDE dont have submission description
            //actually maybe it does as a CVparam...
            
            
            for (Element contact : XMLUtils.getChildrenByName(admin, "contact")) {
                String name = XMLUtils.getChildByName(contact, "name").getTextTrim();
                String institution = XMLUtils.getChildByName(contact, "institution").getTextTrim();
                
                Organization org = new Organization(institution, null, null, null, null);
                if (!st.msi.organizations.contains(org)) {
                    st.msi.organizations.add(org);
                }
                
                String[] splitnames = PRIDEutils.splitName(name);
                Person per = new Person(splitnames[2], splitnames[1], splitnames[0], null, "Submitter");
                if (!st.msi.persons.contains(per)) {
                    st.msi.persons.add(per);
                }
            }
            
            for (Element reference : XMLUtils.getChildrenByName(exp, "Reference")) {
                for (Element referenceadditional : XMLUtils.getChildrenByName(reference, "additional")) {
                    for (Element referenceaddpar : XMLUtils.getChildrenByName(referenceadditional, "cvParam")) {
                        if (referenceaddpar.attributeValue("cvLabel").trim().equals("PubMed")){
                            //some PubMed IDs have full URLs, strip them
                            String pubmedid = referenceaddpar.attributeValue("accession").trim();
                            pubmedid.replace("http://www.ncbi.nlm.nih.gov/pubmed/", "");
                            //TODO check to avoid duplicates
                            st.msi.publications.add(new Publication(pubmedid, null));
                        }
                    }
                }
            }

            DatabaseAttribute dbattr = new DatabaseAttribute("PRIDE", accession, 
                    "https://www.ebi.ac.uk/pride/archive/projects/"+submissionId.substring(4)+"/assays/"+accession);
            
            if (isSubSample(sampledescription)){
                //this is a sub-sample thing
                int subsampleid = 1;
                while (hasSubSample(sampledescription, subsampleid)) {                    
                    //create the sample
                    SampleNode sample = new SampleNode();
                    sample.setNodeName(accession+" subsample "+subsampleid);
                    
                    //TODO this ins't a great link for subsampled XMLs, but best PRIDE can do
                    sample.addAttribute(dbattr);
                    
                    //find the applicable attributes
                    for (Element cvparam : XMLUtils.getChildrenByName(sampledescription, "cvParam")) {
                        SCDNodeAttribute attr = cvParamToAttribute(st, cvparam, subsampleid);
                        if (attr != null){
                            sample.addAttribute(attr);
                        }
                    }
                    
                    for (Element userparam : XMLUtils.getChildrenByName(sampledescription, "userParam")){
                        SCDNodeAttribute attr = userParamToAttribute(st, userparam, subsampleid);
                        if (attr != null){
                            sample.addAttribute(attr);
                        }
                    }
                    
                    //additional information about the samples
                    for (Element cvparam : XMLUtils.getChildrenByName(additional, "cvParam")){
                        SCDNodeAttribute attr = cvParamToAttribute(st, cvparam, subsampleid);
                        if (attr != null){
                            sample.addAttribute(attr);
                        }
                    }
                    
                    for (Element userparam : XMLUtils.getChildrenByName(additional, "userParam")){
                        SCDNodeAttribute attr = userParamToAttribute(st, userparam, subsampleid);
                        if (attr != null){
                            sample.addAttribute(attr);
                        }
                    }
                                     
                    try {
                        st.scd.addNode(sample);
                    } catch (ParseException e) {
                        log.error("Unable to add node "+sample, e);
                        continue;
                    }
                    //move to next subsample
                    subsampleid += 1;   
                }
                
            } else {
                SampleNode sample = new SampleNode();
                //could use this, but no guarantee that this is unique over project
                //String name = samplename.getTextTrim();
                //sample.setNodeName(name);
                sample.setNodeName(accession);
                sample.addAttribute(dbattr);
                
                if (samplename != null){
                    SCDNodeAttribute attr = new CommentAttribute("user name", samplename.getTextTrim());
                    sample.addAttribute(attr);
                }
                
    
                for (Element cvparam : XMLUtils.getChildrenByName(sampledescription, "cvParam")) {
                    SCDNodeAttribute attr = cvParamToAttribute(st, cvparam);
                    if (attr != null){
                        sample.addAttribute(attr);
                    }
                }
                
                for (Element userparam : XMLUtils.getChildrenByName(sampledescription, "userParam")){
                    SCDNodeAttribute attr = userParamToAttribute(st, userparam);
                    if (attr != null){
                        sample.addAttribute(attr);
                    }
                }
                
                //additional information about the samples
                for (Element cvparam : XMLUtils.getChildrenByName(additional, "cvParam")){
                    SCDNodeAttribute attr = cvParamToAttribute(st, cvparam);
                    if (attr != null){
                        sample.addAttribute(attr);
                    }
                }
                
                for (Element userparam : XMLUtils.getChildrenByName(additional, "userParam")){
                    SCDNodeAttribute attr = userParamToAttribute(st, userparam);
                    if (attr != null){
                        sample.addAttribute(attr);
                    }
                }
                
                try {
                    st.scd.addNode(sample);
                } catch (ParseException e) {
                    log.error("Unable to add node "+sample, e);
                    continue;
                }
            }
        }
        

        GroupNode othergroup = new GroupNode("Other Group");
        for (SampleNode sample : st.scd.getNodes(SampleNode.class)) {
            // check there is not an existing group first...
            boolean inGroup = false;
            
            if (!inGroup){
                log.debug("Adding sample " + sample.getNodeName() + " to group " + othergroup.getNodeName());
                othergroup.addSample(sample);
            }
        }
        //only add the new group if it has any samples
        if (othergroup.getParentNodes().size() > 1){
            try {
                st.scd.addNode(othergroup);
                log.info("Added Other group node");
            } catch (ParseException e) {
                log.error("Unable to add node "+othergroup, e);
            }
        }
        
        log.info("Finished convert()");
        return st;
    }

    public void convert(Set<File> infiles, Writer writer, String submissionId) throws IOException, DocumentException, ValidateException  {
        log.debug("recieved infiles, preparing to convert");
        SampleData st = convert(infiles, submissionId);
        log.info("SampleTab converted, preparing to write");

        //Validator<SampleData> validator = new SampleTabValidator();
        //validator.validate(st);

        Normalizer norm = new Normalizer();
        norm.normalize(st);
        
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        sampletabwriter.write(st);
        log.info("SampleTab written");
        sampletabwriter.close();

    }

    public void convert(Set<File> infiles, File sampletabFile) throws IOException, DocumentException, ValidateException {

        // create parent directories, if they dont exist
        sampletabFile = sampletabFile.getAbsoluteFile();
        if (sampletabFile.isDirectory()) {
            sampletabFile = new File(sampletabFile, "sampletab.pre.txt");
        }
        if (!sampletabFile.getParentFile().exists()) {
            sampletabFile.getParentFile().mkdirs();
        }
        String submissionId = sampletabFile.getParentFile().getName();
        FileWriter writer = null;
        try {
            writer = new FileWriter(sampletabFile); 
            convert(infiles, writer, submissionId);
        } finally {
            try {
                if (writer != null ){
                    writer.close();
                }
            } catch (IOException e) {
                //do nothing
            }
        }
    }

}
