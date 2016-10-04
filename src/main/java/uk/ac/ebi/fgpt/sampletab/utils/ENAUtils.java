package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ENAUtils {

    private static Logger log = LoggerFactory.getLogger(ENAUtils.class);
    
    private static LoadingCache<String, Document> lookupDocument = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
        new CacheLoader<String, Document>() {
          public Document load(String id) throws DocumentException, MalformedURLException, IOException {
              String urlstr = "http://www.ebi.ac.uk/ena/data/view/" + id + "&display=xml";
              URL url = new URL(urlstr);
              Document doc = XMLUtils.getDocument(url);
              return doc;
          }
        });
    
    public static Collection<String> getIdentifiers(String input) {
        ArrayList<String> idents = new ArrayList<String>();
        if (input.contains(",")) {
            for (String substr : input.split(",")) {
                idents.add(substr);
            }
        } else {
            idents.add(input);
        }

        ArrayList<String> newidents = new ArrayList<String>();
        for (String ident : idents) {
            if (ident.contains("-")) {
                // its a range
                String[] range = ident.split("-");
                int lower = new Integer(range[0].substring(3));
                int upper = new Integer(range[1].substring(3));
                String prefix = range[0].substring(0, 3);
                for (int i = lower; i <= upper; i++) {
                    newidents.add(String.format(prefix + "%06d", i));
                }
            } else {
                newidents.add(ident);
            }
        }

        return newidents;
    }
    
    public static Document getDocumentById(String srsId)  throws DocumentException, IOException {
        Document doc = null;
        try {
        	doc = lookupDocument.get(srsId);
        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (DocumentException e2) {
                throw e2;
            } catch (MalformedURLException e2) {
                throw e2;
            } catch (IOException e2) {
                throw e2;
            } catch (Throwable e2) {
                throw new RuntimeException("Unrecognised ExecutionException", e2);
            }
        }
    	return doc;
    }
    
    public static Element getElementById(String srsId)  throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupDocument.get(srsId).getRootElement();
        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (DocumentException e2) {
                throw e2;
            } catch (MalformedURLException e2) {
                throw e2;
            } catch (IOException e2) {
                throw e2;
            } catch (Throwable e2) {
                throw new RuntimeException("Unrecognised ExecutionException", e2);
            }
        }
    	return elem;
    }
    
    public static Element getStudyElement(String studyId)  throws DocumentException, IOException, NonPublicObjectException {
    	Element study = getElementById(studyId);
    	if (study == null) {
    		throw new NonPublicObjectException("No public record for "+studyId);
    	}
    	return study;
    }

    public static Element getSubmissionElement(String subId)  throws DocumentException, IOException, NonPublicObjectException {
    	Element submission = getElementById(subId);
    	if (submission == null) {
    		throw new NonPublicObjectException("No public record for "+subId);
    	}
    	return submission;
    }

    public static Element getSampleElement(String srsId) throws DocumentException, IOException, NonPublicObjectException {
    	Element sample = getElementById(srsId);
    	if (sample == null) {
    		throw new NonPublicObjectException("No public record for "+srsId);
    	}
    	return sample;
    }

    public static Element getExperimentElement(String srsId) throws DocumentException, IOException, NonPublicObjectException {
    	Element experiment = getElementById(srsId);
    	if (experiment == null) {
    		throw new NonPublicObjectException("No public record for "+srsId);
    	}
    	return experiment;
    }

    public static Element getRunElement(String srsId) throws DocumentException, IOException, NonPublicObjectException {
    	Element run = getElementById(srsId);
    	if (run == null) {
    		throw new NonPublicObjectException("No public record for "+srsId);
    	}
    	return run;
    }

    public static Set<String> getStudiesForSample(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getStudiesForSample(getSampleElement(srsId));
    }

    public static Set<String> getStudiesForSample(Element sample) {
        Set<String> studyIDs = new HashSet<String>();
        Element links = XMLUtils.getChildByName(sample, "SAMPLE_LINKS");
        if (links != null) {
            for (Element link : XMLUtils.getChildrenByName(links, "SAMPLE_LINK")) {
                Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
                if (xref != null) {
                    Element db = XMLUtils.getChildByName(xref, "DB");
                    Element id = XMLUtils.getChildByName(xref, "ID");
                    if (db != null && db.getText().equals("ENA-STUDY") && id != null) {
                        studyIDs.addAll(getIdentifiers(id.getText()));
                    }
                }
            }
        }
        return studyIDs;
    }
    
    public static Set<String> getSamplesForStudy(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getSamplesForStudy(getStudyElement(srsId));
    }

    public static Set<String> getSamplesForStudy(Element study) {
        Set<String> sampleIDs = new HashSet<String>();
        for (Element studyLinks : XMLUtils.getChildrenByName(study, "STUDY_LINKS")) {
            for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "STUDY_LINK")) {
                for (Element xrefLink : XMLUtils.getChildrenByName(studyLink, "XREF_LINK")) {
                    Element db = XMLUtils.getChildByName(xrefLink, "DB");
                    Element id = XMLUtils.getChildByName(xrefLink, "ID");
                    if (db.getText().equals("ENA-SAMPLE")) {
                        if (db != null && db.getText().equals("ENA-SAMPLE") && id != null) {
                            log.debug("Processing samples "+id.getText() );
                            sampleIDs.addAll(getIdentifiers(id.getText()));
                        }
                    }
                }
            }
        }
        return sampleIDs;
    }
    
    
    public static String getSubmissionForSample(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getSubmissionForSample(getSampleElement(srsId));
    }

    public static String getSubmissionForSample(Element sample) {
        List<String> studyIds = new ArrayList<String>(1);
        Element links = XMLUtils.getChildByName(sample, "SAMPLE_LINKS");
        if (links != null) {
            for (Element link : XMLUtils.getChildrenByName(links, "SAMPLE_LINK")) {
                Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
                if (xref != null) {
                    Element db = XMLUtils.getChildByName(xref, "DB");
                    Element id = XMLUtils.getChildByName(xref, "ID");
                    if (db != null && db.getText().equals("ENA-SUBMISSION") && id != null) {
                        studyIds.addAll(getIdentifiers(id.getText()));
                    }
                }
            }
        } 
        //check that only one submission identifier is present
        if (studyIds.size() != 1) {
        	throw new RuntimeException("Found more than one submission id for "+sample.attributeValue("accession"));
        }
        
        return studyIds.get(0);
    }
    
    
    public static String getSubmissionForStudy(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getSubmissionForStudy(getStudyElement(srsId));
    }

    public static String getSubmissionForStudy(Element study) {
        List<String> studyIds = new ArrayList<String>(1);
        Element links = XMLUtils.getChildByName(study, "STUDY_LINKS");
        if (links != null) {
            for (Element link : XMLUtils.getChildrenByName(links, "STUDY_LINK")) {
                Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
                if (xref != null) {
                    Element db = XMLUtils.getChildByName(xref, "DB");
                    Element id = XMLUtils.getChildByName(xref, "ID");
                    if (db != null && db.getText().equals("ENA-SUBMISSION") && id != null) {
                        studyIds.addAll(getIdentifiers(id.getText()));
                    }
                }
            }
        }
        //check that only one submission identifier is present
        if (studyIds.size() != 1) {
        	throw new RuntimeException("Found more than one submission id for "+study.attributeValue("accession"));
        }
        
        return studyIds.get(0);
    }

    public static Set<String> getStudiesForSubmission(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getStudiesForSubmission(getSubmissionElement(srsId));
    }

    public static Set<String> getStudiesForSubmission(Element submission) {
        Set<String> studyIDs = new HashSet<String>();
        Element links = XMLUtils.getChildByName(submission, "SUBMISSION_LINKS");
        if (links != null) {
            for (Element link : XMLUtils.getChildrenByName(links, "SUBMISSION_LINK")) {
                Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
                if (xref != null) {
                    Element db = XMLUtils.getChildByName(xref, "DB");
                    Element id = XMLUtils.getChildByName(xref, "ID");
                    if (db != null && db.getText().equals("ENA-STUDY") && id != null) {
                        studyIDs.addAll(getIdentifiers(id.getText()));
                    }
                }
            }
        }
        return studyIDs;
    }
    
    public static Set<String> getSamplesForSubmission(String srsId) throws DocumentException, IOException, NonPublicObjectException {
        return getSamplesForSubmission(getSubmissionElement(srsId));
    }

    public static Set<String> getSamplesForSubmission(Element submission) {
        Set<String> sampleIDs = new HashSet<String>();
        if ( XMLUtils.getChildrenByName(submission, "SUBMISSION_LINKS") == null 
        		||  XMLUtils.getChildrenByName(submission, "SUBMISSION_LINKS").size() == 0) {
        	log.info("No SUBMISSION_LINKS found");
        }
        for (Element studyLinks : XMLUtils.getChildrenByName(submission, "SUBMISSION_LINKS")) {
        	
            if ( XMLUtils.getChildrenByName(studyLinks, "SUBMISSION_LINK") == null
            		||  XMLUtils.getChildrenByName(studyLinks, "SUBMISSION_LINK").size() == 0) {
            	log.info("No SUBMISSION_LINK found");
            }
            for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "SUBMISSION_LINK")) {
            	
                if ( XMLUtils.getChildrenByName(studyLink, "XREF_LINK") == null
                		||  XMLUtils.getChildrenByName(studyLink, "XREF_LINK").size() == 0){ 
                	log.info("No XREF_LINK found");
                }
                for (Element xrefLink : XMLUtils.getChildrenByName(studyLink, "XREF_LINK")) {
                    Element db = XMLUtils.getChildByName(xrefLink, "DB");
                    Element id = XMLUtils.getChildByName(xrefLink, "ID");
                    log.info(db.getText()+" "+id.getText());
                    if (db.getTextTrim().equals("ENA-SAMPLE")) {
                        if (db != null && db.getTextTrim().equals("ENA-SAMPLE") && id != null) {
                            log.info("Processing sample "+id.getText() );
                            sampleIDs.addAll(getIdentifiers(id.getText()));
                        }
                    }
                }
            }
        }
        return sampleIDs;
    }
    
    public static Set<String> getSamplesForExperiment(String srxId) throws DocumentException, IOException, NonPublicObjectException {
        return getSamplesForExperiment(getExperimentElement(srxId));
    }

    public static Set<String> getSamplesForExperiment(Element experiment) {
        Set<String> sampleIDs = new HashSet<String>();
        for (Element studyLinks : XMLUtils.getChildrenByName(experiment, "EXPERIMENT_LINKS")) {
            for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "EXPERIMENT_LINK")) {
                for (Element xrefLink : XMLUtils.getChildrenByName(studyLink, "XREF_LINK")) {
                    Element db = XMLUtils.getChildByName(xrefLink, "DB");
                    Element id = XMLUtils.getChildByName(xrefLink, "ID");
                    if (db.getText().equals("ENA-SAMPLE")) {
                        if (db != null && db.getText().equals("ENA-SAMPLE") && id != null) {
                            log.debug("Processing samples "+id.getText() );
                            sampleIDs.addAll(getIdentifiers(id.getText()));
                        }
                    }
                }
            }
        }
        return sampleIDs;
    }
    
    public static Set<String> getSamplesForRun(String srrId) throws DocumentException, IOException, NonPublicObjectException {
        return getSamplesForRun(getRunElement(srrId));
    }

    public static Set<String> getSamplesForRun(Element run) {
        Set<String> sampleIDs = new HashSet<String>();
        for (Element studyLinks : XMLUtils.getChildrenByName(run, "RUN_LINKS")) {
            for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "RUN_LINK")) {
                for (Element xrefLink : XMLUtils.getChildrenByName(studyLink, "XREF_LINK")) {
                    Element db = XMLUtils.getChildByName(xrefLink, "DB");
                    Element id = XMLUtils.getChildByName(xrefLink, "ID");
                    if (db.getText().equals("ENA-SAMPLE")) {
                        if (db != null && db.getText().equals("ENA-SAMPLE") && id != null) {
                            log.debug("Processing samples "+id.getText() );
                            sampleIDs.addAll(getIdentifiers(id.getText()));
                        }
                    }
                }
            }
        }
        return sampleIDs;
    }
    
    public static Set<String> getSecondaryAccessions(String enaId)  throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupDocument.get(enaId).getRootElement();
        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (DocumentException e2) {
                throw e2;
            } catch (MalformedURLException e2) {
                throw e2;
            } catch (IOException e2) {
                throw e2;
            } catch (Throwable e2) {
                throw new RuntimeException("Unrecognised ExecutionException", e2);
            }
        }
        return getSecondaryAccessions(elem);
    }
    
    public static Set<String> getSecondaryAccessions(Element root) {
        Set<String> secondarys = new HashSet<String>();
        for (Element entry : XMLUtils.getChildrenByName(root, "entry")) {
            for (Element secondary : XMLUtils.getChildrenByName(entry, "secondaryAccession")) {
                secondarys.add(secondary.getTextTrim());
            }
        }
        return secondarys;
    }

    public static String getBioSampleIdForSample(String enaId) throws DocumentException, IOException, UnrecognizedBioSampleException, MissingBioSampleException, NonPublicObjectException {
    	if (!enaId.matches("[ESD]RS[0-9]+")) { 
    		throw new IllegalArgumentException(""+enaId+" is not a valid identifier");
    	}
        return getBioSampleIdForSample(getSampleElement(enaId));
    }
    
    public static String getBioSampleIdForSample(Element sampleElement) throws UnrecognizedBioSampleException, MissingBioSampleException {
    	
    	if (sampleElement == null) {
    		throw new IllegalArgumentException("sampleElement cannot be null");
    	}
    	
    	String biosampleId = null;
        Element identifiers = XMLUtils.getChildByName(sampleElement, "IDENTIFIERS");
        for (Element otherId : XMLUtils.getChildrenByName(identifiers, "EXTERNAL_ID")) {
        	if ("BioSample".equals(otherId.attributeValue("namespace"))) {
        		if (biosampleId == null) {
        			biosampleId = otherId.getTextTrim();
        			
        		} else if (biosampleId.equals(otherId.getTextTrim())) {
        			//its the same ID multiple times, ignore it
        		} else {
        			//TODO make explicit exception
                    throw new RuntimeException("Multiple BioSample IDs in "+sampleElement.attributeValue("accession"));
        		}
        	}
        }
        
        if (biosampleId == null) {
        	throw new MissingBioSampleException("Missing biosample accession in "+sampleElement.attributeValue("accession"));
        }
        
        if (!biosampleId.matches("SAM[END][AG]?[0-9]+")) {
        	throw new UnrecognizedBioSampleException("Unrecognized biosample accession "+biosampleId);        
        }
        
        return biosampleId;
    }
    
    public static class MissingBioSampleException extends Exception {

		public MissingBioSampleException(String string) {
			super(string);
		}
    	
    }
    
    public static class UnrecognizedBioSampleException extends Exception {

		public UnrecognizedBioSampleException(String string) {
			super(string);
		}
    	
    }
    
    public static class NonPublicObjectException extends Exception {

		public NonPublicObjectException(String string) {
			super(string);
		}
    }
    
}
