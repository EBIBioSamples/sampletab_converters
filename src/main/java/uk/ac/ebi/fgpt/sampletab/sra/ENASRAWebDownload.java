package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.custommonkey.xmlunit.Diff;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.util.NodeComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class ENASRAWebDownload {

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    public ENASRAWebDownload() {
        
    }


    public boolean download(String accession, String outdir) throws DocumentException, IOException {
        return this.download(accession, new File(outdir));
    }

    public boolean download(String accession, File outdir) throws DocumentException, IOException {
        // TODO check accession is actually an ENA SRA study accession

        String url = "http://www.ebi.ac.uk/ena/data/view/" + accession + "&display=xml";

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
        boolean sampleWriteOut = true;
        
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
                //do nothing, file will be overwritten
                e.printStackTrace();
            } catch (SAXException e) {
                //do nothing, file will be overwritten
                e.printStackTrace();
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
            String sampleURL = "http://www.ebi.ac.uk/ena/data/view/" + sampleSRAAccession + "&display=xml";
            File sampleFile = new File(outdir.getAbsoluteFile(), sampleSRAAccession + ".xml");
            Document sampledoc = XMLUtils.getDocument(sampleURL);
            
            sampleWriteOut = false;
            
            if (!sampleFile.exists()){
                //if it does not exist, it needs to be written
                sampleWriteOut = true;
            } else {
                sampleWriteOut = true;
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
                        sampleWriteOut = false;
                    }
                } catch (ParserConfigurationException e) {
                    //do nothing, file will be overwritten
                } catch (SAXException e) {
                    //do nothing, file will be overwritten
                }
            }
            
            if (sampleWriteOut){
                //update the study write out variable if any of the samples were updated
                //this is so it can return the correct value
                
                studyWriteOut = true;
                log.info("Downloading "+sampleSRAAccession+" to disk");
                //write the sample xml to disk
                os = null;
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
                        os.close();
                    }
                }
            } else {
                log.debug("Skipping "+sampleSRAAccession);
            }
        }
        log.debug("ENA SRA study download complete.");
        return studyWriteOut;

    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Must provide an ENA SRA study accession and an output directory.");
            return;
        }
        String accession = args[0];
        String outdir = args[1];

        ENASRAWebDownload downloader = new ENASRAWebDownload();
        try {
            downloader.download(accession, outdir);
        } catch (DocumentException e) {
            System.err.println("Unable to download "+accession+" to "+outdir);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Unable to download "+accession+" to "+outdir);
            e.printStackTrace();
        }

    }
}
