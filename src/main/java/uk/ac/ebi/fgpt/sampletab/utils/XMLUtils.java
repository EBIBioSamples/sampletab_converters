package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.DocumentSource;
import org.dom4j.io.SAXReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLUtils {
    private static Logger log = LoggerFactory.getLogger("XMLUtils");

	private static ConcurrentLinkedQueue<SAXReader> readerQueue = new ConcurrentLinkedQueue<SAXReader>();

	public static Document getDocument(File xmlFile) throws FileNotFoundException, DocumentException {
        return getDocument(new BufferedReader(new FileReader(xmlFile)));
	}

	public static Document getDocument(URL url) throws DocumentException, IOException {        
        URLConnection conn = null;
        if (System.getProperty("proxySet") != null) {
            String hostname = System.getProperty("proxyHost");
            int port = Integer.parseInt(System.getProperty("proxyPort"));
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(hostname, port));
    	    conn = url.openConnection(proxy);
        } else {
            conn = url.openConnection();
        }
       
	    Reader r = null;
	    Document doc = null;
	    try {
	        r = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	        doc = getDocument(r);
	    } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
	    }
	    return doc;
	}

    public static Document getDocument(String xmlString) throws DocumentException {
        Reader r = null;
        Document doc = null;
        try {
            r = new StringReader(xmlString);
            doc = getDocument(r);
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        return doc;
    }
    
    public static Document getDocument(Reader r) throws DocumentException {
        SAXReader reader = readerQueue.poll();
        if (reader == null) {
            reader = new SAXReader();
        }
        
        //now do actual parsing
        Document xml = null;
        
        xml = reader.read(r);
        //return the reader back to the queue
        reader.resetHandlers();
        readerQueue.add(reader);
        
        return xml;
    }

	public static Element getChildByName(Element parent, String name) {
		if (parent == null)
			return null;

		for (Iterator<Element> i = parent.elementIterator(); i.hasNext();) {
			Element child = i.next();
			if (child.getName().equals(name)) {
				return child;
			}
		}

		return null;
	}

	public static Collection<Element> getChildrenByName(Element parent,
			String name) {
		Collection<Element> children = new ArrayList<Element>();

		if (parent == null)
			return children;

		for (Iterator<Element> i = parent.elementIterator(); i.hasNext();) {
			Element child = i.next();
			if (child.getName().equals(name)) {
				children.add(child);
			}
		}
		return children;
	}
    
    public static String stripNonValidXMLCharacters(String in) {
        //from http://blog.mark-mclaren.info/2007/02/invalid-xml-characters-when-valid-utf8_5873.html

        if (in == null){ 
            return null;
        }
        
        StringBuffer out = new StringBuffer(); // Used to hold the output.
        char current; // Used to reference the current character.
        
        for (int i = 0; i < in.length(); i++) {
            current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
            if ((current == 0x9) ||
                (current == 0xA) ||
                (current == 0xD) ||
                ((current >= 0x20) && (current <= 0xD7FF)) ||
                ((current >= 0xE000) && (current <= 0xFFFD)) ||
                ((current >= 0x10000) && (current <= 0x10FFFF))){
                out.append(current);
            }
        }
        return out.toString();
    } 

    public static org.w3c.dom.Document convertDocument(Document orig) throws TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer t = tf.newTransformer();
        DOMResult result = new DOMResult();
        t.transform(new DocumentSource(orig), result);
        return (org.w3c.dom.Document) result.getNode();
    }
}
