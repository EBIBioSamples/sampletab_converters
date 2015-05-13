package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.dom4j.Document;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.validator.Validator;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ERAUpdateCallable implements Callable<Void> {

    private final String submissionId;
    
    private final File outDir;
    
    private final boolean conan;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private static TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    //characteristics that we want to ignore
    private static Collection<String> characteristicsIgnore;
    static {
        characteristicsIgnore = new TreeSet<String>();
        characteristicsIgnore.add("ENA-SPOT-COUNT");
        characteristicsIgnore.add("ENA-BASE-COUNT");
        characteristicsIgnore.add("ENA-SUBMISSION-TOOL");
        characteristicsIgnore.add("ENA-CHECKLIST");
        characteristicsIgnore = Collections.unmodifiableCollection(characteristicsIgnore);
    }
    
	public ERAUpdateCallable(File outDir, String submissionId, boolean conan) {
		this.outDir = outDir;
		this.submissionId = submissionId;
		this.conan = conan;
	}

	@Override
	public Void call() throws Exception {
        
        SampleData st = new SampleData();
        st.msi.submissionIdentifier = "GEN-"+submissionId;
        
		//get the samples
		for (String sampleId : ENAUtils.getSamplesForSubmission(submissionId)) {			

            Element sampleroot = ENAUtils.getElementById(sampleId);
            Element sampleElement = XMLUtils.getChildByName(sampleroot, "SAMPLE");
            Element sampleName = XMLUtils.getChildByName(sampleElement, "SAMPLE_NAME");
            Element sampledescription = XMLUtils.getChildByName(sampleElement, "DESCRIPTION");
            Element synonym = XMLUtils.getChildByName(sampleElement, "TITLE");
            
            //sometimes the study is public but the samples are private
            //check for that and skip sample
            if (sampleElement == null){
                continue;
            }
            // check that this actually is the sample we want
            if (sampleElement.attributeValue("accession") != null
                    && !sampleElement.attributeValue("accession").equals(sampleId)) {
                throw new ParseException("Accession in XML content does not match filename");
            }

            // create the actual sample node
            SampleNode samplenode = new SampleNode();
            samplenode.setNodeName(sampleId);
            

            // process any synonyms that may exist
            Element taxon = XMLUtils.getChildByName(sampleName, "TAXON_ID");
            Element indivname = XMLUtils.getChildByName(sampleName, "INDIVIDUAL_NAME");
            Element scientificname = XMLUtils.getChildByName(sampleName, "SCIENTIFIC_NAME");
            Element annonname = XMLUtils.getChildByName(sampleName, "ANONYMIZED_NAME");
            
            // insert all synonyms at position zero so they display next to name
            if (indivname != null) {
                CommentAttribute synonymattrib = new CommentAttribute("Synonym", indivname.getTextTrim());
                samplenode.addAttribute(synonymattrib, 0);
            }
            if (annonname != null) {
                CommentAttribute synonymattrib = new CommentAttribute("Synonym", annonname.getTextTrim());
                samplenode.addAttribute(synonymattrib, 0);
            }
            if (synonym != null) {
                CommentAttribute synonymattrib = new CommentAttribute("Synonym", synonym.getTextTrim());
                samplenode.addAttribute(synonymattrib, 0);
            }
            
            
            //colect various IDs together
            Element identifiers = XMLUtils.getChildByName(sampleElement, "IDENTIFIERS");
            Collection<Element> otherIDs = XMLUtils.getChildrenByName(identifiers, "EXTERNAL_ID");
            otherIDs.addAll(XMLUtils.getChildrenByName(identifiers, "SUBMITTER_ID"));
            otherIDs.addAll(XMLUtils.getChildrenByName(identifiers, "SECONDARY_ID"));
            for (Element id : otherIDs) {
                //these should be database ids rather than synonyms when possible
                if (samplenode.getSampleAccession() == null && 
                        id.getTextTrim().matches("SAM[END][A]?[0-9]+")) {
                    //this is a biosamples accession, and we dont have one yet, so use it
                    samplenode.setSampleAccession(id.getTextTrim());
                    
                    //also use the accession as the database link
                    samplenode.addAttribute(new DatabaseAttribute("ENA SRA",
                    		id.getTextTrim(), 
                            "http://www.ebi.ac.uk/ena/data/view/" + id.getTextTrim()));
                    
                } else if (samplenode.getSampleAccession() != null && 
                        samplenode.getSampleAccession().equals(id.getTextTrim())) {
                    //same as the existing sample accession
                    //do nothing
                } else if (samplenode.getSampleAccession() != null && 
                        id.getTextTrim().matches("SAM[END][A]?[0-9]+") &&
                        !samplenode.getSampleAccession().equals(id.getTextTrim())){
                    //this is a biosamples accession, but we already have one, report an error and store as synonym
                    log.error("Strange biosample identifiers in "+sampleId);
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", id.getTextTrim());
                    samplenode.addAttribute(synonymattrib, 0);                        
                } else {
                    //store it as a synonym
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", id.getTextTrim());
                    samplenode.addAttribute(synonymattrib, 0);
                }
            }
            
            //process any alias present
            String alias = sampleElement.attributeValue("alias");
            if (alias != null) {
                alias = alias.trim();
                if (samplenode.getSampleAccession() == null && 
                        alias.matches("SAM[END][A]?[0-9]+")) {
                    //this is a biosamples accession, and we dont have one yet, so use it
                    samplenode.setSampleAccession(alias);
                } else if (samplenode.getSampleAccession() != null && 
                        alias.matches("SAM[END][A]?[0-9]+") &&
                        !samplenode.getSampleAccession().equals(alias)){
                    //this is a biosamples accession, but we already have one, report an error and store as synonym
                    log.error("Strange biosample identifiers in "+sampleId);
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", alias);
                    samplenode.addAttribute(synonymattrib, 0);                        
                } else {
                    //store it as a synonym
                    CommentAttribute synonymattrib = new CommentAttribute("Synonym", alias);
                    samplenode.addAttribute(synonymattrib, 0);
                }
            }
            
            // now process organism
            if (taxon != null) {
                Integer taxid = new Integer(taxon.getTextTrim());
                // could get taxon name by lookup, but this will be done by correction later on.
                String taxName = null;
                if (scientificname != null ) {
                    taxName = scientificname.getTextTrim();
                } else {
                    taxName = taxid.toString();
                }
                
                OrganismAttribute organismAttribute = null;
                if (taxName != null && taxid != null) {
                    organismAttribute = new OrganismAttribute(taxName, st.msi.getOrAddTermSource(ncbitaxonomy), taxid);
                } else if (taxName != null) {
                    organismAttribute = new OrganismAttribute(taxName);
                }

                if (organismAttribute != null) {
                    samplenode.addAttribute(organismAttribute);
                }
            }

            if (sampleElement.attributeValue("alias") != null) {
                CommentAttribute synonymattrib = new CommentAttribute("Synonym", sampleElement.attributeValue("alias"));
                samplenode.addAttribute(synonymattrib, 0);
            }
            
            if (sampledescription != null) {
                String descriptionstring = sampledescription.getTextTrim();
                samplenode.setSampleDescription(descriptionstring);
            }

            // finally, any other attributes ENA SRA provides
            Element sampleAttributes = XMLUtils.getChildByName(sampleElement, "SAMPLE_ATTRIBUTES");
            if (sampleAttributes != null) {
                for (Element sampleAttribute : XMLUtils.getChildrenByName(sampleAttributes, "SAMPLE_ATTRIBUTE")) {
                    Element tag = XMLUtils.getChildByName(sampleAttribute, "TAG");
                    Element value = XMLUtils.getChildByName(sampleAttribute, "VALUE");
                    Element units = XMLUtils.getChildByName(sampleAttribute, "UNITS");
                    String tagtext;
                    
                    if (tag == null || tag.getTextTrim().length() == 0) {
                        tagtext = "unknown";
                    } else {
                        tagtext = tag.getTextTrim();
                    }
                        
                    if (characteristicsIgnore.contains(tagtext)) {
                        //skip this characteristic
                        log.debug("Skipping characteristic attribute "+tagtext);
                        continue;
                    }
                    
                    String valuetext;
                    if (value == null) {
                        // some ENA SRA attributes are boolean
                        //mark them as unknown
                        valuetext = tagtext;
                        tagtext = "unknown";
                    } else {
                        valuetext = value.getTextTrim();
                    }
                    CharacteristicAttribute characteristicAttribute = new CharacteristicAttribute(tagtext,
                            valuetext);
                    
                    if (units != null && units.getTextTrim().length() > 0) {
                        log.trace("Added unit "+units.getTextTrim());
                        characteristicAttribute.unit = new UnitAttribute();
                        characteristicAttribute.unit.setAttributeValue(units.getTextTrim());
                    }

                    samplenode.addAttribute(characteristicAttribute);
                }
            }
            
            samplenode.addAttribute(new DatabaseAttribute("ENA SRA", sampleId, "http://www.ebi.ac.uk/ena/data/view/" + sampleId));

            st.scd.addNode(samplenode);
            
		}
		
		//get the groups
		for (String studyId : ENAUtils.getStudiesForSubmission(submissionId)) {
            Element root = ENAUtils.getElementById(studyId);
            
            Element mainElement = XMLUtils.getChildByName(root, "STUDY");
            Element descriptor = XMLUtils.getChildByName(mainElement, "DESCRIPTOR");
            
            String description = null;
            if (XMLUtils.getChildByName(descriptor, "STUDY_ABSTRACT") != null) {
                description = XMLUtils.getChildByName(descriptor, "STUDY_ABSTRACT").getTextTrim();
            } else if (XMLUtils.getChildByName(descriptor, "STUDY_DESCRIPTION") != null) {
                description = XMLUtils.getChildByName(descriptor, "STUDY_DESCRIPTION").getTextTrim();
            } else if (XMLUtils.getChildByName(descriptor, "STUDY_TITLE") != null) {
            	description = XMLUtils.getChildByName(descriptor, "STUDY_TITLE").getTextTrim();
            } else {
                log.warn("no STUDY_ABSTRACT or STUDY_DESCRIPTION");
            }

            
            GroupNode groupNode = new GroupNode(studyId);
            if (description != null) {
            	groupNode.setGroupDescription(description);
            }

            groupNode.addAttribute(new DatabaseAttribute("ENA SRA", studyId, "http://www.ebi.ac.uk/ena/data/view/" + studyId));

            //study links
            
            
            for (String id : ENAUtils.getSamplesForStudy(studyId)) {
        		SampleNode sampleNode = st.scd.getNode(id, SampleNode.class);
        		if (sampleNode == null) {
        			//study refers to sample in other submission
        			sampleNode = new SampleNode(id);
        			String biosampleId = ENAUtils.getBioSampleIdForSample(id);
        			sampleNode.setSampleAccession(biosampleId);
        			st.scd.addNode(sampleNode);
                    groupNode.addSample(sampleNode);
        		} else {
                    groupNode.addSample(sampleNode);
        		}
            	
            }
            
            
            
            st.scd.addNode(groupNode);
		}

        log.trace("SampleTab converted, preparing to write");

        Validator<SampleData> validator = new SampleTabValidator();
        validator.validate(st);

        Normalizer norm = new Normalizer();
        norm.normalize(st);

        File outsubdir = new File(outDir, SampleTabUtils.getSubmissionDirPath(st.msi.submissionIdentifier));
        outsubdir.mkdirs();
        File file = new File(outsubdir, "sampletab.pre.txt");

        SampleTabWriter sampletabwriter = null;
        try {
	        sampletabwriter = new SampleTabWriter(new BufferedWriter(new FileWriter(file.getAbsolutePath())));
	        sampletabwriter.write(st);
	        log.trace("SampleTab written");
        } finally {
        	sampletabwriter.close();
        }
        

        //trigger conan if appropriate
        if (conan) {
            ConanUtils.submit(st.msi.submissionIdentifier, "BioSamples (other)");
        }
        
		return null;
	}

}
