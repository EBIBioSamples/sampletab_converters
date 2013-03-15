package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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

    private static LoadingCache<String, Element> lookupElement = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
        new CacheLoader<String, Element>() {
          public Element load(String id) throws DocumentException, MalformedURLException, IOException {
              String urlstr = "http://www.ebi.ac.uk/ena/data/view/" + id + "&display=xml";
              URL url = new URL(urlstr);
              Document doc = XMLUtils.getDocument(url);

              Element root = doc.getRootElement();
              return root;
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

    public static Set<String> getStudiesForSample(String srsId) throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupElement.get(srsId);
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
        return getStudiesForSample(elem);
    }

    public static Set<String> getStudiesForSample(Element root) {
        Set<String> studyIDs = new HashSet<String>();
        Element sample = XMLUtils.getChildByName(root, "SAMPLE");
        if (sample != null) {
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
        }
        return studyIDs;
    }
    
    public static Set<String> getSamplesForStudy(String srsId) throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupElement.get(srsId);
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
        return getSamplesForStudy(elem);
    }

    public static Set<String> getSamplesForStudy(Element root) {
        Set<String> sampleIDs = new HashSet<String>();
        for (Element study : XMLUtils.getChildrenByName(root, "STUDY")) {
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
        }
        return sampleIDs;
    }
    
    public static Set<String> getSubmissionsForSample(String srsId) throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupElement.get(srsId);
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
        return getSubmissionsForSample(elem);
    }

    public static Set<String> getSubmissionsForSample(Element root) {
        Set<String> studyIDs = new HashSet<String>();
        Element sample = XMLUtils.getChildByName(root, "SAMPLE");
        if (sample != null) {
            Element links = XMLUtils.getChildByName(sample, "SAMPLE_LINKS");
            if (links != null) {
                for (Element link : XMLUtils.getChildrenByName(links, "SAMPLE_LINK")) {
                    Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
                    if (xref != null) {
                        Element db = XMLUtils.getChildByName(xref, "DB");
                        Element id = XMLUtils.getChildByName(xref, "ID");
                        if (db != null && db.getText().equals("ENA-SUBMISSION") && id != null) {
                            studyIDs.addAll(getIdentifiers(id.getText()));
                        }
                    }
                }
            }
        }
        return studyIDs;
    }

    public static Set<String> getStudiesForSubmission(String srsId) throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupElement.get(srsId);
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
        return getStudiesForSubmission(elem);
    }

    public static Set<String> getStudiesForSubmission(Element root) {
        Set<String> studyIDs = new HashSet<String>();
        Element sample = XMLUtils.getChildByName(root, "SUBMISSION");
        if (sample != null) {
            Element links = XMLUtils.getChildByName(sample, "SUBMISSION_LINKS");
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
        }
        return studyIDs;
    }
    
    public static Set<String> getSamplesForSubmission(String srsId) throws DocumentException, IOException {
        Element elem = null;
        try {
            elem = lookupElement.get(srsId);
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
        return getSamplesForSubmission(elem);
    }

    public static Set<String> getSamplesForSubmission(Element root) {
        Set<String> sampleIDs = new HashSet<String>();
        for (Element study : XMLUtils.getChildrenByName(root, "SUBMISSION")) {
            for (Element studyLinks : XMLUtils.getChildrenByName(study, "SUBMISSION_LINKS")) {
                for (Element studyLink : XMLUtils.getChildrenByName(studyLinks, "SUBMISSION_LINK")) {
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
        }
        return sampleIDs;
    }
    
}
