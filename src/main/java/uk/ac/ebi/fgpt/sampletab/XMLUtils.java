package uk.ac.ebi.fgpt.sampletab;

import java.util.ArrayList;
import java.util.Collection;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLUtils {
	
	public static Element getChildByName(Element parent, String name) {
		NodeList nodes = parent.getChildNodes();

		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);

			if (node instanceof Element) {
				// a child element to process
				Element child = (Element) node;
				if (child.getNodeName() == name) {
					return child;
				}
			}
		}
		return null;
	}
	
	public static Collection<Element> getChildrenByName(Element parent, String name) {
		Collection<Element> children = new ArrayList<Element>();
		
		NodeList nodes = parent.getChildNodes();

		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);

			if (node instanceof Element) {
				// a child element to process
				Element child = (Element) node;
				if (child.getNodeName() == name) {
					children.add(child);
				}
			}
		}
		return children;
	}
	
}
