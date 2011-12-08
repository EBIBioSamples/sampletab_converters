package uk.ac.ebi.fgpt.sampletab;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.dom4j.Element;

public class XMLUtils {

	public static Element getChildByName(Element parent, String name) {
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
		for (Iterator<Element> i = parent.elementIterator(); i.hasNext();) {
			Element child = i.next();
			if (child.getName().equals(name)) {
				children.add(child);
			}
		}
		return children;
	}

}
