package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * 
 * Utility class that reads an input stream of XML (with SAX) and calls a provided
 * handler for each element of interest. The handler is given a DOM populated
 * element to do something with 
 * 
 * 
 * @author faulcon
 *
 */
public class XMLFragmenter {
	private SAXParser saxParser ;
	private DocumentBuilder docBuilder;
	private DefaultHandler handler;
	
	private XMLFragmenter() {
		// private constructor
	}
	
	public static XMLFragmenter newInstance() throws ParserConfigurationException, SAXException {
		XMLFragmenter fragmenter = new XMLFragmenter();
		fragmenter.setup();
		return fragmenter;
	}

	private void setup() throws ParserConfigurationException, SAXException {

		SAXParserFactory factory = SAXParserFactory.newInstance();
		saxParser = factory.newSAXParser();
		
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		docBuilder = docBuilderFactory.newDocumentBuilder();
	}
	
	public void handleStream(InputStream inputStream, String encoding, ElementCallback callback) throws ParserConfigurationException, SAXException, IOException {

		InputSource isource = new InputSource(inputStream);
		isource.setEncoding(encoding);
		

	    DefaultHandler handler = new FragmentationHandler(docBuilder, callback);
	    
		saxParser.parse(isource, handler);
	}

	private class FragmentationHandler extends DefaultHandler {
 
		
		private final DocumentBuilder docBuilder;
		private final ElementCallback callback;
		
		private org.w3c.dom.Document doc = null;
		private boolean inRegion = false;
	    private Stack<Element> elementStack = new Stack<Element>();
	    private StringBuilder textBuffer = new StringBuilder();
		
		public FragmentationHandler(DocumentBuilder docBuilder, ElementCallback callback) {
			 this.docBuilder = docBuilder;
			 this.callback = callback;
		}
		
		@Override
		public void startElement(String uri, String localName,
				String qName, Attributes attributes) throws SAXException {
			//System.out.println("> "+inBioSample+" "+uri+" "+localName+" "+qName);
			if (callback.isBlockStart(uri, localName, qName, attributes)) {
				inRegion = true;
				doc = docBuilder.newDocument();
			}
			if (inRegion) {
				addTextIfNeeded();
				Element el = doc.createElement(qName);
				for (int i = 0; i < attributes.getLength(); i++)
					el.setAttribute(attributes.getQName(i),
							attributes.getValue(i));
				elementStack.push(el);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			//System.out.println("< "+inBioSample+" "+uri+" "+localName+" "+qName);
			if (inRegion) {
				addTextIfNeeded();
				Element closedEl = elementStack.pop();
				
				if (elementStack.isEmpty()) {
					
					doc.appendChild(closedEl);
					
					//do something with the element	
					callback.handleElement(doc.getDocumentElement());

					inRegion = false;
					doc = null;
					
				} else {
					Element parentEl = elementStack.peek();
					parentEl.appendChild(closedEl);
				}			
			}
		}

		@Override
		public void characters(char ch[], int start, int length)
				throws SAXException {
			if (inRegion) {
				textBuffer.append(ch, start, length);
			}
		}

		// Outputs text accumulated under the current node
		private void addTextIfNeeded() {
			if (textBuffer.length() > 0) {
				Element el = elementStack.peek();
				Node textNode = doc.createTextNode(textBuffer.toString());
				el.appendChild(textNode);
				textBuffer.delete(0, textBuffer.length());
			}
		}
	};
	
	public interface ElementCallback {
		public void handleElement(Element e);
		
		public boolean isBlockStart(String uri, String localName,
				String qName, Attributes attributes);
	}
}
