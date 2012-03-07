package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ENASRAXMLToSampleTab {

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
    
    //characteristics that we want to ignore
    private static Collection<String> characteristicsIgnore;
    static {
        characteristicsIgnore = new TreeSet<String>();
        characteristicsIgnore.add("ENA-SPOT-COUNT");
        characteristicsIgnore.add("ENA-BASE-COUNT");
        characteristicsIgnore = Collections.unmodifiableCollection(characteristicsIgnore);
    }
    
    public ENASRAXMLToSampleTab() {
        
    }

    public SampleData convert(String filename) throws IOException, ParseException {
        return convert(new File(filename));
    }

    public SampleData convert(File infile) throws ParseException, IOException {
        
        infile = infile.getAbsoluteFile();
        
        if (infile.isDirectory()) {
            infile = new File(infile, "study.xml");
            log.info("Given a directly, looking for " + infile);
        }
        

        if (!infile.exists()) {
            throw new IOException(infile + " does not exist");
        }

        Document studydoc;
        try {
            studydoc = XMLUtils.getDocument(infile);
        } catch (DocumentException e) {
            // rethrow as a ParseException
            throw new ParseException("Unable to parse XML document of study");
        }
        Element root = studydoc.getRootElement();
        Element study = XMLUtils.getChildByName(root, "STUDY");

        SampleData st = new SampleData();
        // title
        Element descriptor = XMLUtils.getChildByName(study, "DESCRIPTOR");
        st.msi.submissionIdentifier = XMLUtils.getChildByName(descriptor, "STUDY_TITLE").getTextTrim();
        st.msi.submissionTitle = "GEN-"+study.attributeValue("accession");

        // description
        String description = null;
        if (descriptor != null) {
            Element studyAbstract = XMLUtils.getChildByName(descriptor, "STUDY_ABSTRACT");
            Element studyDescription = XMLUtils.getChildByName(descriptor, "STUDY_DESCRIPTION");
            if (studyAbstract != null) {
                description = studyAbstract.getTextTrim();
            } else if (studyDescription != null) {
                description = studyDescription.getTextTrim();
                // } else if (pmids.size() > 0){
                // log.warn("no STUDY_ABSTRACT or STUDY_DESCRIPTION, falling back to PubMedID");
                // //TODO implement
            } else {
                log.warn("no STUDY_ABSTRACT or STUDY_DESCRIPTION");
            }
        }
        if (description != null) {
            st.msi.submissionDescription = description;
        }

        // pubmed link
        Set<Integer> pmids = new TreeSet<Integer>();
        for (Element studyLinks : XMLUtils.getChildrenByName(study, "STUDY_LINKS")) {
            for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "STUDY_LINK")) {
                for (Element xrefLink : XMLUtils.getChildrenByName(studyLink, "XREF_LINK")) {
                    Element db = XMLUtils.getChildByName(xrefLink, "DB");
                    Element id = XMLUtils.getChildByName(xrefLink, "ID");
                    if (db.getTextTrim().equals("PUBMED")) {
                        pmids.add(new Integer(id.getTextTrim()));
                    }
                }
            }
        }
        for (Integer pmid : pmids) {
            st.msi.publicationPubMedID.add("" + pmid);
            st.msi.publicationDOI.add("");
        }

        // database link
        st.msi.databases.add(new Database("ENA SRA", 
                "http://www.ebi.ac.uk/ena/data/view/" + study.attributeValue("accession"),
                study.attributeValue("accession")));

        // organization
        Element centerName = XMLUtils.getChildByName(descriptor, "CENTER_NAME");
        Element centerTitle = XMLUtils.getChildByName(descriptor, "CENTER_TITLE");
        Element centerProjectName = XMLUtils.getChildByName(descriptor, "CENTER_PROJECT_NAME");

        if (centerName != null) {
            st.msi.organizationName.add(centerName.getTextTrim());
        } else if (centerTitle != null) {
            st.msi.organizationName.add(centerTitle.getTextTrim());
        } else if (study.attributeValue("center_name") != null) {
            st.msi.organizationName.add(study.attributeValue("center_name"));
        } else if (centerProjectName != null) {
            st.msi.organizationName.add(centerProjectName.getTextTrim());
        } else {
            throw new ParseException("Unable to find organization name.");
        }

        st.msi.organizationRole.add("Submitter");
        // ENA SRA does not make emails available
        st.msi.organizationEmail.add("");
        // ENA SRA does not make web sites available
        st.msi.organizationURI.add("");

        //ENA SRA does not have explicit term sources
        //Put a couple on by default
        
        st.msi.termSourceName.add("NEWT");
        st.msi.termSourceURI.add("http://www.ebi.ac.uk/newt");
        st.msi.termSourceVersion.add("");
        
        st.msi.termSourceName.add("EFO");
        st.msi.termSourceURI.add("http://www.ebi.ac.uk/efo");
        st.msi.termSourceVersion.add("");
        
        log.info("MSI section complete, starting SCD section.");

        // start on the samples
        Set<String> sampleSRAAccessions = ENAUtils.getSamplesForStudy(root);
        File indir = infile.getParentFile();
        for (String sampleSRAAccession : sampleSRAAccessions) {
            File sampleFile = new File(indir, sampleSRAAccession + ".xml");

            Document sampledoc;
            try {
                sampledoc = XMLUtils.getDocument(sampleFile);
            } catch (DocumentException e) {
                // rethrow as a ParseException
                throw new ParseException("Unable to parse XML document of sample " + sampleSRAAccession);
            }
            Element sampleroot = sampledoc.getRootElement();
            Element sampleElement = XMLUtils.getChildByName(sampleroot, "SAMPLE");
            Element sampleName = XMLUtils.getChildByName(sampleElement, "SAMPLE_NAME");
            Element sampledescription = XMLUtils.getChildByName(sampleElement, "DESCRIPTION");

            // check that this actually is the sample we want
            if (sampleElement.attributeValue("accession") != null
                    && !sampleElement.attributeValue("accession").equals(sampleSRAAccession)) {
                throw new ParseException("Accession in XML content does not match filename");
            }

            log.info("Processing sample " + sampleSRAAccession);

            // create the actual sample node
            SampleNode samplenode = new SampleNode();
            samplenode.setNodeName(sampleSRAAccession);

            // process any synonyms that may exist
            // ignore species names and accession duplicates
            Element synonym;
            synonym = XMLUtils.getChildByName(sampleElement, "TITLE");
            if (synonym != null && !synonym.getTextTrim().equals(sampleSRAAccession)
                    && !synonym.getTextTrim().equals(XMLUtils.getChildByName(sampleName, "SCIENTIFIC_NAME").getTextTrim())) {
                CommentAttribute synonymattrib = new CommentAttribute("Synonym", synonym.getTextTrim());
                // insert all synonyms at position zero so they display next
                // to name
                samplenode.addAttribute(synonymattrib, 0);
            }
            if (sampleName != null) {
                synonym = XMLUtils.getChildByName(sampleName, "INDIVIDUAL_NAME");
                if (synonym != null) {
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", synonym.getTextTrim());
                    // insert all synonyms at position zero so they display next to name
                    samplenode.addAttribute(synonymattrib, 0);
                }

                synonym = XMLUtils.getChildByName(sampleName, "ANONYMIZED_NAME");
                if (synonym != null) {
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", synonym.getTextTrim());
                    samplenode.addAttribute(synonymattrib, 0);
                }
            }
            if (sampledescription != null) {
                if (sampledescription.attributeValue("alias") != null) {
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym",
                            sampledescription.attributeValue("alias"));
                    samplenode.addAttribute(synonymattrib, 0);
                }
            }

            // now process organism
            if (sampleName != null) {
                Element taxon = XMLUtils.getChildByName(sampleName, "TAXON_ID");
                Element scientificname = XMLUtils.getChildByName(sampleName, "SCIENTIFIC_NAME");
                Element commonname = XMLUtils.getChildByName(sampleName, "COMMON_NAME");
                Integer taxid = null;
                String taxName = null;
                if (taxon != null) {
                    taxid = new Integer(taxon.getTextTrim());
                    // TODO get taxon name from id
                }
                if (scientificname != null) {
                    taxName = scientificname.getTextTrim();
                } else if (commonname != null) {
                    taxName = commonname.getTextTrim();
                }

                OrganismAttribute organismAttribute = null;
                if (taxName != null && taxid != null) {
                    organismAttribute = new OrganismAttribute(taxName, "NEWT", taxid);
                } else if (taxName != null) {
                    organismAttribute = new OrganismAttribute(taxName);
                }

                if (organismAttribute != null) {
                    samplenode.addAttribute(organismAttribute);
                }
            }

            // finally, any other attributes ENA SRA provides
            Element sampleAttributes = XMLUtils.getChildByName(sampleElement, "SAMPLE_ATTRIBUTES");
            if (sampleAttributes != null) {
                for (Element sampleAttribute : XMLUtils.getChildrenByName(sampleAttributes, "SAMPLE_ATTRIBUTE")) {
                    Element tag = XMLUtils.getChildByName(sampleAttribute, "TAG");
                    Element value = XMLUtils.getChildByName(sampleAttribute, "VALUE");
                    Element units = XMLUtils.getChildByName(sampleAttribute, "UNITS");
                    if (tag != null) {
                        
                        String tagtext = tag.getTextTrim();
                        
                        if (characteristicsIgnore.contains(tagtext)){
                            //skip this characteristic
                            log.debug("Skipping characteristic attribute "+tagtext);
                            continue;
                        }
                        
                        //TODO deal with Sex correctly
                        
                        String valuetext;
                        if (value == null) {
                            // some ENA SRA attributes are boolean
                            valuetext = tagtext;
                        } else {
                            valuetext = value.getTextTrim();
                        }
                        CharacteristicAttribute characteristicAttribute = new CharacteristicAttribute(tagtext,
                                valuetext);
                        if (units != null) {
                            // TODO deal with units on characteristics
                        }

                        samplenode.addAttribute(characteristicAttribute);
                    }
                }
            }

            st.scd.addNode(samplenode);
        }

        log.info("Finished convert()");
        return st;
    }

    public void convert(File file, Writer writer) throws IOException, ParseException {
        log.debug("recieved xml, preparing to convert");
        SampleData st = convert(file);

        log.info("SampleTab converted, preparing to write");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        sampletabwriter.write(st);
        log.info("SampleTab written");
        sampletabwriter.close();

    }

    public void convert(File studyFile, String outfilename) throws IOException, ParseException {

        convert(studyFile, new File(outfilename));
    }

    public void convert(File studyFile, File sampletabFile) throws IOException, ParseException {

        // create parent directories, if they dont exist
        sampletabFile = sampletabFile.getAbsoluteFile();
        if (sampletabFile.isDirectory()) {
            sampletabFile = new File(sampletabFile, "sampletab.pre.txt");
        }
        if (!sampletabFile.getParentFile().exists()) {
            sampletabFile.getParentFile().mkdirs();
        }

        convert(studyFile, new FileWriter(sampletabFile));
    }

    public void convert(String studyFilename, Writer writer) throws IOException, ParseException {
        convert(new File(studyFilename), writer);
    }

    public void convert(String studyFilename, File sampletabFile) throws IOException, ParseException {
        convert(studyFilename, new FileWriter(sampletabFile));
    }

    public void convert(String studyFilename, String sampletabFilename) throws IOException, ParseException {
        convert(studyFilename, new File(sampletabFilename));
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Must provide an ENA SRA study filename and a SampleTab output filename.");
            return;
        }
        String enasrafilename = args[0];
        String sampleTabFilename = args[1];

        ENASRAXMLToSampleTab converter = new ENASRAXMLToSampleTab();

        try {
            converter.convert(enasrafilename, sampleTabFilename);
        } catch (ParseException e) {
            System.out.println("Error converting " + enasrafilename + " to " + sampleTabFilename);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Error converting " + enasrafilename + " to " + sampleTabFilename);
            e.printStackTrace();
        }
    }
}
