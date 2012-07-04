package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class XMLUtils {

	private static ConcurrentLinkedQueue<SAXReader> readerQueue = new ConcurrentLinkedQueue<SAXReader>();

	public synchronized static Document getDocument(File xmlFile) throws FileNotFoundException, DocumentException{
		SAXReader reader = readerQueue.poll();
		if (reader == null){
			reader = new SAXReader();
		}
		
		//now do actual parsing
		Document xml = null;
		Reader r = null;
		try {
			r = new BufferedReader(new FileReader(xmlFile));
			xml = reader.read(r);
		} finally {
			if (r != null){
				try {
					r.close();
				} catch (IOException e){
					//do nothing
				}
			}
			//return the reader back to the queue
			reader.resetHandlers();
			readerQueue.add(reader);
		}
		
		return xml;
	}

	public static Document getDocument(String xmlURL) throws DocumentException {
		SAXReader reader = readerQueue.poll();
		if (reader == null) {
			reader = new SAXReader();
		}

		// now do actual parsing
		Document xml = null;
		try {
			xml = reader.read(xmlURL);
		} finally {
			// return the reader back to the queue
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

}
