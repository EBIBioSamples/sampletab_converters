package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class XMLUtils {

	private static ConcurrentLinkedQueue<SAXReader> readerQueue = new ConcurrentLinkedQueue<SAXReader>();

	public static Document getDocument(File xmlFile) throws FileNotFoundException, DocumentException {
        return getDocument(new BufferedReader(new FileReader(xmlFile)));
	}

	public static Document getDocument(URL url) throws DocumentException, IOException {
	    //use the client to work with proxies rather than doing more naive option
	    DefaultHttpClient httpclient = new DefaultHttpClient();
	    HttpGet httpGet = new HttpGet(url.toString());
	    HttpResponse response = httpclient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
        }
 
        return getDocument(new BufferedReader( new InputStreamReader((response.getEntity().getContent()))));
	}

    public static Document getDocument(String xmlString) throws DocumentException {
        return getDocument(new StringReader(xmlString));
    }
    
    public static Document getDocument(Reader r) throws DocumentException {
        SAXReader reader = readerQueue.poll();
        if (reader == null) {
            reader = new SAXReader();
        }
        
        //now do actual parsing
        Document xml = null;
        try {
            xml = reader.read(r);
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
            //return the reader back to the queue
            reader.resetHandlers();
            readerQueue.add(reader);
        }
        
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

}
