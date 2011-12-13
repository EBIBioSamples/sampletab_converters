package uk.ac.ebi.fgpt.sampletab.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.dom4j.Element;

public class XMLUtils {

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
