package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.TransformerException;

import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;
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
import uk.ac.ebi.fgpt.sampletab.Accessioner;
import uk.ac.ebi.fgpt.sampletab.Normalizer;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils.MissingBioSampleException;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils.NonPublicObjectException;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils.UnrecognizedBioSampleException;
import uk.ac.ebi.fgpt.sampletab.utils.OLSUtils;
import uk.ac.ebi.fgpt.sampletab.utils.OLSUtils.TooManyIRIsException;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ERAUpdateCallable implements Callable<Void> {

    private final String submissionId;
    
    private final File outDir;
    
    private final boolean conan;
    
    private final Accessioner accessioner;
    
    private final boolean force;
    
    private final SampleData st = new SampleData();

    private static Validator<SampleData> validator = new SampleTabValidator();
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private boolean updated = false;
    
    private static final Pattern attributeWithOntology = Pattern.compile("(.*) \\((.*)\\)");

    private static TermSource ncbitaxonomy = new TermSource("NCBI Taxonomy", "http://www.ncbi.nlm.nih.gov/taxonomy/", null);
    
    private final ERADAO eradao;
    
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
    
	public ERAUpdateCallable(File outDir, String submissionId, boolean conan, Accessioner accessioner, boolean force, ERADAO eradao) {
		this.outDir = outDir;
		this.submissionId = submissionId;
		this.conan = conan;
		this.accessioner = accessioner;
		this.force = force;
		this.eradao = eradao;
	}
	
	private void handleSample(String sampleId, Document sampleDocument) throws ParseException {
		
		if (!sampleId.startsWith("ERS")) {
			log.info("Skipping "+sampleId+" because it doesn't start ERS");
			return;
		}
		

         
        Element sampleroot = sampleDocument.getRootElement();
        Element sampleElement = XMLUtils.getChildByName(sampleroot, "SAMPLE");
        
        //if its a null link, then private a do not add
        if (sampleElement == null) {
        	log.info("Skipping "+sampleId+" because it has no SAMPLE XML element");
        	return;
        }
        
        Element sampleName = XMLUtils.getChildByName(sampleElement, "SAMPLE_NAME");
        Element sampledescription = XMLUtils.getChildByName(sampleElement, "DESCRIPTION");
        Element synonym = XMLUtils.getChildByName(sampleElement, "TITLE");
        
        //sometimes the study is public but the samples are private
        //check for that and skip sample
        if (sampleElement == null) {
            return;
        }
        // check that this actually is the sample we want
        if (sampleElement.attributeValue("accession") != null
                && !sampleElement.attributeValue("accession").equals(sampleId)) {
            throw new ParseException("Accession in XML content does not match filename");
        }

        // create the actual sample node
        SampleNode samplenode = new SampleNode(sampleId);
        try {
			samplenode.setSampleAccession(ENAUtils.getBioSampleIdForSample(sampleElement));
		} catch (UnrecognizedBioSampleException e) {
			log.error("Problem with "+sampleId, e);
			return;
		} catch (MissingBioSampleException e) {
			log.error("Problem with "+sampleId, e);
			return;
		}
        

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
            if (samplenode.getSampleAccession() != null && 
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

                CharacteristicAttribute characteristicAttribute = new CharacteristicAttribute(tagtext, valuetext);
                //some ENA SRA attributes may have ontology terms included 
                Matcher m = attributeWithOntology.matcher(valuetext);
                if (m.matches()) {
                	String ontologyId = m.group(2);
                    
					Optional<URI> ontologyIri = Optional.empty();
                    try {
                    	ontologyIri = OLSUtils.guessIRIfromShortTerm(ontologyId);
					} catch (IOException | URISyntaxException | TooManyIRIsException e) {
						log.error("Unable to guess IRI from short term "+ontologyId, e);
					}
                    if (ontologyIri.isPresent()) {
                    	log.info("Adding TermSourceID to "+tagtext+":"+valuetext+" "+ontologyIri.get());

                        characteristicAttribute = new CharacteristicAttribute(tagtext,
                        		m.group(1));
                    	characteristicAttribute.setTermSourceID(ontologyIri.get().toString());
                    }
                }                
                
                if (units != null && units.getTextTrim().length() > 0) {
                    log.trace("Added unit "+units.getTextTrim());
                    characteristicAttribute.unit = new UnitAttribute();
                    characteristicAttribute.unit.setAttributeValue(units.getTextTrim());
                }

                samplenode.addAttribute(characteristicAttribute);
            }
        }
        
        //if it was assigned a non-EBI accession, stop
        //non-EBI should be part of NCBI import
        if (samplenode.getSampleAccession() != null && !samplenode.getSampleAccession().startsWith("SAME")) {
        	log.info("Skipping "+samplenode.getSampleAccession()+" because its not EBI");
        	return;
        }
        
        //if this is a sample which has biosamples authority, stop
        //these will be direct submissions
        if (samplenode.getSampleAccession() != null && eradao.getBioSamplesAuthority(samplenode.getSampleAccession())) {
        	log.info("Skipping "+samplenode.getSampleAccession()+" because its a biosample accession");
        	return;
        }
        
        samplenode.addAttribute(new DatabaseAttribute("ENA SRA", sampleId, "http://www.ebi.ac.uk/ena/data/view/" + sampleId));

        st.scd.addNode(samplenode);
	}

	private void referenceSample(String sampleId, Document sampleDocument) throws ParseException {
		//need a reference to the sample, so name and accession only

        Element sampleroot = sampleDocument.getRootElement();
        Element sampleElement = XMLUtils.getChildByName(sampleroot, "SAMPLE");
        
        //if its a null link, then private a do not add
        if (sampleElement == null) {
        	return;
        }
        
        // create the actual sample node
        SampleNode samplenode = new SampleNode(sampleId);
        try {
			samplenode.setSampleAccession(ENAUtils.getBioSampleIdForSample(sampleElement));
		} catch (UnrecognizedBioSampleException e) {
			log.error("Problem with "+sampleId, e);
			return;
		} catch (MissingBioSampleException e) {
			log.error("Problem with "+sampleId, e);
			return;
		}
        
        st.scd.addNode(samplenode);
	}
	
	@Override
	public Void call() throws Exception {
        
		log.info("processing started for "+submissionId);
		
        st.msi.submissionIdentifier = "GEN-"+submissionId;
        
        Document subDocument = getDocumentIfUpdated(submissionId);
        
        Element submissionElement = XMLUtils.getChildByName(subDocument.getRootElement(), "SUBMISSION");
        
		//get the samples
		for (String sampleId : ENAUtils.getSamplesForSubmission(submissionElement)) {	
			Document sampleDocument = getDocumentIfUpdated(sampleId);
			Element root = sampleDocument.getRootElement();
            Element sampleElement = XMLUtils.getChildByName(root, "SAMPLE");
            
            //sometimes a sample will be referred to that doesn't exist - check for that here
            if (sampleElement != null) {
				//check that this sample is for this submission
            	
            	//log.info(""+submissionId+" vs "+ENAUtils.getSubmissionForSample(sampleElement));
            	
				if (submissionId.equals(ENAUtils.getSubmissionForSample(sampleElement))) {
					
					//this sample is owned by this submission
					handleSample(sampleId, sampleDocument);
				} else {
					//this sample is referenced by this submission, but not included in it
					referenceSample(sampleId, sampleDocument);
				}
            }
			
            
		}
		
		//get the groups
		for (String studyId : ENAUtils.getStudiesForSubmission(submissionElement)) {		

			Document studyDocument = getDocumentIfUpdated(studyId);
			Element root = studyDocument.getRootElement();
            Element studyElement = XMLUtils.getChildByName(root, "STUDY");
	        //if its a null link, then private a do not add
	        if (studyElement == null) {
	        	continue;
	        }

            GroupNode groupNode = new GroupNode(studyId);
            //groups need to have an accession assigned to them
            groupNode.setGroupAccession(accessioner.singleGroup(studyId, "ENA"));
            
            String studySubmissionId = ENAUtils.getSubmissionForStudy(studyElement);
			//check that this group is in this submission
			if (submissionId.equals(studySubmissionId)) {
				//owned by this submission
	            Element descriptor = XMLUtils.getChildByName(studyElement, "DESCRIPTOR");
	            
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

	            
	            if (description != null) {
	            	groupNode.setGroupDescription(description);
	            }

	            groupNode.addAttribute(new DatabaseAttribute("ENA SRA", studyId, "http://www.ebi.ac.uk/ena/data/view/" + studyId));

	            //study links
	            for (String id : ENAUtils.getSamplesForStudy(studyElement)) {
	        		SampleNode sampleNode = st.scd.getNode(id, SampleNode.class);
        			//check if this study refers to samples in another submission
	        		if (sampleNode == null) {
	        			sampleNode = new SampleNode(id);
        				String biosampleId = null;
	        			try {
	        				biosampleId = ENAUtils.getBioSampleIdForSample(id);
	        			} catch (NonPublicObjectException e) {
	        				//group in this submission refers to another submissions sample that is not public
	        				//skip it
	        				continue;
	        			}
	        			sampleNode.setSampleAccession(biosampleId);
	        			
	        			st.scd.addNode(sampleNode);
	                    groupNode.addSample(sampleNode);
	                    
	        		} else {
	                    groupNode.addSample(sampleNode);
	        		}
	            }
			} else {
				//referenced by this submission, but owned by another
				//group node already has name and accession
				//need to add links to samples in this submission

				//NOTE: datamodel doesn't really handle studies having multiple owners, so disabling this for now
				log.info("Not adding "+groupNode.getGroupAccession()+" because it is owned by "+studySubmissionId);
				/*
	            for (String id : ENAUtils.getSamplesForStudy(studyElement)) {
	        		SampleNode sampleNode = st.scd.getNode(id, SampleNode.class);
        			//check if this study refers to samples in another submission
	        		if (sampleNode == null) {
	                    //in this case, do nothing
	        		} else {
	                    groupNode.addSample(sampleNode);
	        		}
	            }
	            */
			}

			//only add the group if the group contains samples
			if (groupNode.getParentNodes().size() > 0) {
				st.scd.addNode(groupNode);
			}
		}

		//only write out and trigger conan if there was an update

        File outsubdir = new File(outDir, SampleTabUtils.getSubmissionDirPath(st.msi.submissionIdentifier));
        File file = new File(outsubdir, "sampletab.pre.txt");
        
		if (updated || !file.exists() || force){
	        
	        synchronized(validator) {
	        	validator.validate(st);
	        }
	
	        Normalizer norm = new Normalizer();
	        norm.normalize(st);

	        if (st.scd.getAllNodes().size() > 0 ) {	   
				
		        log.info("SampleTab converted, preparing to write "+submissionId);     
		        outsubdir.mkdirs();	
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
	        } else {
	        	log.info("Not writing to "+submissionId+" because it has no content");
	        }
        
		}
		log.info("processing finished for "+submissionId);
		//have to return something from this function
		return null;
	}

	private Document getDocumentIfUpdated(String accession) throws DocumentException, IOException {
		//get the document from the web service
		Document newDoc = ENAUtils.getDocumentById(accession);

        File outsubdir = new File(outDir, SampleTabUtils.getSubmissionDirPath("GEN-"+submissionId));
        outsubdir.mkdirs();
        File localCopy = new File(outsubdir, accession+".xml");
        if (localCopy.exists()) {
    		//get the copy from disk (if present)        	
	        Document existingDoc = XMLUtils.getDocument(localCopy);

	        //diff the XML files
	        XMLUnit.setIgnoreAttributeOrder(true);
	        XMLUnit.setIgnoreWhitespace(true);

		    //need them to be in the right class for XMLTest to use
	        org.w3c.dom.Document docOrig = null;
	        org.w3c.dom.Document docNew = null;
	        try {
	            docOrig = XMLUtils.convertDocument(existingDoc);
	            docNew = XMLUtils.convertDocument(newDoc);
	        } catch (TransformerException e) {
	            log.error("Unable to convert from dom4j to w3c Document");
	        }
	        
	        Diff diff = new Diff(docOrig, docNew);
	        if (diff.similar()) {
	            //equivalent to last file, no update needed
	            return existingDoc;
	        }
        }
        //either the disk version was missing or different, so its an update
    	updated = true;

    	//save the new/updated version to disk
        OutputStream os = null;
        XMLWriter writer = null;
        try {
            os = new BufferedOutputStream(new FileOutputStream(localCopy));
            //this pretty printing is messing up comparisons by trimming whitespace WITHIN an element
            //OutputFormat format = OutputFormat.createPrettyPrint();
            //XMLWriter writer = new XMLWriter(os, format);
            writer = new XMLWriter(os);
            writer.write(newDoc);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
            
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
    	
    	return newDoc;
	}
	
}
