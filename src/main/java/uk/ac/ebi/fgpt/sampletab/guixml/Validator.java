package uk.ac.ebi.fgpt.sampletab.guixml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class Validator extends AbstractInfileDriver {
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public class BiosamplesHandler extends DefaultHandler {
        
        private String groupID = null;
        private final Collection<String> sampleattributesknown = new ArrayList<String>();
        private boolean inSample = false;
        private boolean inGroup = false;
        private String attributeClass = null;
        private final Collection<String> sampleattributesseen = new HashSet<String>();
        private final Collection<String> sampleattributesseensample = new ArrayList<String>();

        public BiosamplesHandler() {
            
        }
        
        //TODO
        //ensure every sample has a "Sample Accession" attribute
        //endsure very group has update and release dates in the correct format YYYY/MM/DD
        
        @Override
        public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
            if (name.equals("SampleGroup")) {
                groupID = atts.getValue("id");
                //check group ID
                if (groupID == null) {
                    throw new SAXException("missing group ID");
                } else if (!groupID.matches("SAM[END][AG]?[1-9][0-9]{4,}")) {
                    throw new SAXException(groupID+" is not a valid group ID");
                }
                inSample = false;
                inGroup = true;
            } else if (name.equals("SampleAttributes")) {
                if (sampleattributesknown.size() != 0) {
                    throw new SAXException("Multiple SampleAttributes in "+groupID);
                }
                //TODO FINISH ME
            } else if (name.equals("SampleAttribute")) { //TODO fix - no such thing as SampleAttribute
                String className = atts.getValue("class");
                if (className == null ) {
                    throw new SAXException("Missing SampleAttribute class in "+groupID);
                } else if (sampleattributesknown.contains(className)) {
                    //check sampleAttributes contain no duplicates
                    throw new SAXException("Duplicated SampleAttribute class "+className+" in "+groupID);
                } else {
                    sampleattributesknown.add(className);
                }
            } else if (name.equals("Sample")) {
                inSample = true;
                inGroup = false;
                //check group id
                String sampleGroupID = atts.getValue("groupId");
                if (!groupID.equals(sampleGroupID)) {
                    throw new SAXException("Mismatched groupId : "+groupID+" vs "+sampleGroupID);
                }
                //check sample id
                String sampleID = atts.getValue("id");
                if (!sampleID.matches("SAM[END][AG]?[1-9][0-9]{4,}")) {
                    throw new SAXException(sampleID+" is not a valid sample ID");
                }
            } else if (name.equals("attribute") && inSample) {
                String className = atts.getValue("class");
                if (className == null ) {
                    throw new SAXException("Missing SampleAttribute class in "+groupID);
                } else if (className.equals("Term Source REF") 
                        || className.equals("Term Source ID")
                        || className.equals("Term Source Name")
                        || className.equals("Term Source URI")) {
                    //do nothing
                    //these can be duplicated within child elements, too hard to properly check for
                } else if (sampleattributesseensample.contains(className)) {
                    throw new SAXException("SampleAttribute class "+className+" duplicated in "+groupID);
                } else {
                    sampleattributesseensample.add(className);
                }                            
            } 
        }


        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            if (name.equals("SampleGroup")) {
                groupID = null;
                sampleattributesknown.clear();
                inGroup = false;
            } else if (name.equals("Sample")) {
                inSample = false;
                inGroup = true;
                sampleattributesseen.addAll(sampleattributesseensample);
                sampleattributesseensample.clear();
            } else if (name.equals("SampleAttribute")) {
                //seen but not known
                for (String attr : sampleattributesseen) {
                    if (!sampleattributesknown.contains(attr)) {
                        throw new SAXException("SampleAttribute class "+attr+" is seen but not known "+groupID);   
                    }
                }
                
                //known but not seen
                for (String attr : sampleattributesknown) {
                    if (!sampleattributesseen.contains(attr)) {
                        throw new SAXException("SampleAttribute class "+attr+" is known but not seen "+groupID);   
                    }
                }
                
            } 
        }
    }

    
    private class ValidatorRunnable implements Callable<Void> {
        private final File inputFile;
        
        public ValidatorRunnable(File inputFile) {
            this.inputFile = inputFile;
        }

        @Override
        public Void call() throws Exception {
            //TODO finish
            XMLReader xr;
            BiosamplesHandler handler = new BiosamplesHandler();
            try {
                xr = XMLReaderFactory.createXMLReader();
                xr.setContentHandler(handler);
                xr.parse(new InputSource(new BufferedReader(new FileReader(inputFile))));
            } catch (SAXException e) {
                log.error("Unable to process "+inputFile, e);
                throw e;
            } catch (FileNotFoundException e) {
                log.error("Unable to process "+inputFile, e);
                throw e;
            } catch (IOException e) {
                log.error("Unable to process "+inputFile, e);
                throw e;
            }
            return null;
        }
    }
    
    @Override
    protected Callable<Void> getNewTask(File inputFile) {
        return new ValidatorRunnable(inputFile);
    }
    
    public static void main(String[] args) {
        new Validator().doMain(args);
    }

}
