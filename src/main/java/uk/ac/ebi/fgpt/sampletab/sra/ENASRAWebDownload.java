package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
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

    static {
        XMLUnit.setIgnoreAttributeOrder(true);
        XMLUnit.setIgnoreWhitespace(true);
    }
    
    public ENASRAWebDownload() {
        //Nothing to do in constructor
    }
    
    public boolean downloadXML(String accession, File outdir) throws IOException, DocumentException {
        URL url = new URL("http://www.ebi.ac.uk/ena/data/view/" + accession + "&display=xml");
        File sampleFile = new File(outdir.getAbsoluteFile(), accession + ".xml");
        Document sampledoc = XMLUtils.getDocument(url);
        
        if (!sampleFile.exists()){
            //if it does not exist, it needs to be written
        } else {
            //compare the document in memory with the document on disk
            //first need them to be in the right class for XMLTest to use
            
            Document existingDoc = XMLUtils.getDocument(sampleFile);
            XMLUnit.setIgnoreAttributeOrder(true);

            org.w3c.dom.Document docOrig = null;
            org.w3c.dom.Document docNew = null;
            try {
                docOrig = XMLUtils.convertDocument(existingDoc);
                docNew = XMLUtils.convertDocument(sampledoc);
            } catch (TransformerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            Diff diff = new Diff(docOrig, docNew);
            if (diff.similar()) {
                //equivalent to last file, no update needed
                return false;
            }
        }
        
        log.info("Downloading "+accession+" to disk");
        outdir.mkdirs();
        //write the xml to disk
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
