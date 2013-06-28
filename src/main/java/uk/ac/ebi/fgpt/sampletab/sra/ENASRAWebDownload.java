package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.custommonkey.xmlunit.Diff;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ENASRAWebDownload {
    private Logger log = LoggerFactory.getLogger(getClass());

    public ENASRAWebDownload() {
        //Nothing to do in constructor
    }

    public boolean download(String accession, String outdir) throws DocumentException, IOException {
        return this.download(accession, new File(outdir));
    }

    public boolean download(String accession, File outdir) throws DocumentException, IOException {
        // TODO check accession is actually an ENA SRA study accession

        URL url = new URL("http://www.ebi.ac.uk/ena/data/view/" + accession + "&display=xml");

        log.debug("Prepared for download "+accession);

        Document studyDoc = XMLUtils.getDocument(url);
        Element root = studyDoc.getRootElement();
        
        
        //if this is a blank study, abort
        if (XMLUtils.getChildrenByName(root, "STUDY").size() == 0) {
            log.debug("Blank study, skipping");
            return false;
        }

        //check there is at least 1 sample in the study
        if (ENAUtils.getSamplesForStudy(root).size() == 0){
            log.debug("No samples in study, skipping");
            return false;
        }
            
            
        // create parent directories, if they dont exist
        File studyFile = new File(outdir.getAbsoluteFile(), "study.xml");
        if (!studyFile.getParentFile().exists()) {
            studyFile.getParentFile().mkdirs();
        }
        
        //check that it does not already exist
        boolean studyWriteOut = true;
        
        if (studyFile.exists()){
            //conpare the document in memory with the document on disk
            //first need them to be in the right class for XMLTest to use
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder;
            try {
                builder = factory.newDocumentBuilder();
                org.w3c.dom.Document docA = builder.parse(new InputSource(new StringReader(studyDoc.asXML())));
                org.w3c.dom.Document docB = builder.parse(studyFile);
                Diff diff = new Diff(docA, docB);
                if (diff.similar()){
                    studyWriteOut = false;
                }
            } catch (ParserConfigurationException e) {
                log.warn("Problem with parser configuration", e);
            } catch (SAXException e) {
                log.warn("Problem with SAX parsing", e);
            }
        }
        

        OutputStream os = null;
        if(studyWriteOut){
            log.info("Writing "+accession+" to disk");
            
            //write the study file to disk
            os = null;
            try {
                os = new BufferedOutputStream(new FileOutputStream(studyFile));
                //this pretty printing is messing up comparisons by trimming whitespace WITHIN an element
                //OutputFormat format = OutputFormat.createPrettyPrint();
                //XMLWriter writer = new XMLWriter(os, format);
                XMLWriter writer = new XMLWriter(os);
                writer.write(studyDoc);
                writer.flush();
                os.close();
            } finally {
                if (os != null) {
                    os.close();
                }
            }
        } else {
            log.debug("Skipping "+accession);
        }
        
        Set<String> sampleSRAAccessions = ENAUtils.getSamplesForStudy(root);
        // now there is a set of sample accessions they each need to be retrieved.
        log.debug("Prepared for ENA SRA sample XML download.");
        for (String sampleSRAAccession : sampleSRAAccessions) {
            studyWriteOut |= downloadXML(sampleSRAAccession, outdir);
        }
        log.debug("ENA SRA study download complete.");
        return studyWriteOut;
    }
    
    public boolean downloadXML(String sampleID, File outdir) throws IOException, DocumentException{
        URL url = new URL("http://www.ebi.ac.uk/ena/data/view/" + sampleID + "&display=xml");
        File sampleFile = new File(outdir.getAbsoluteFile(), sampleID + ".xml");
        Document sampledoc = XMLUtils.getDocument(url);
        
        if (!sampleFile.exists()){
            //if it does not exist, it needs to be written
        } else {
            //compare the document in memory with the document on disk
            //first need them to be in the right class for XMLTest to use
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder;
            try {
                builder = factory.newDocumentBuilder();
                org.w3c.dom.Document docA = builder.parse(new InputSource(new StringReader(sampledoc.asXML())));
                org.w3c.dom.Document docB = builder.parse(sampleFile);
                Diff diff = new Diff(docA, docB);
                if (diff.similar()){
                    //equivalent to last file, no update needed
                    return false;
                }
            } catch (ParserConfigurationException e) {
                //do nothing, file will be overwritten
            } catch (SAXException e) {
                //do nothing, file will be overwritten
            }
        }
        
        log.info("Downloading "+sampleID+" to disk");
        outdir.mkdirs();
        //write the sample xml to disk
        OutputStream os = null;
        try {
            os = new BufferedOutputStream(new FileOutputStream(sampleFile));
            //this pretty printing is messing up comparisons by trimming whitespace WITHIN an element
            //OutputFormat format = OutputFormat.createPrettyPrint();
            //XMLWriter writer = new XMLWriter(os, format);
            XMLWriter writer = new XMLWriter(os);
            writer.write(sampledoc);
            writer.flush();
            os.close();
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        return true;
    }
}
