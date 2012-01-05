package uk.ac.ebi.fgpt.sampletab.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class AttributeSummary {

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());
	
	private volatile Map<String, Map<String, Integer>> comments = Collections
			.synchronizedMap(new HashMap<String, Map<String, Integer>>());
	private volatile Map<String, Map<String, Integer>> characteristics = Collections
			.synchronizedMap(new HashMap<String, Map<String, Integer>>());

	public synchronized void addFrom(SampleData sampledata) {
		for (SCDNode node : sampledata.scd.getAllNodes()) {
			for (SCDNodeAttribute attribute : node.getAttributes()) {
				Map<String, Map<String, Integer>> map = null;
				if (CommentAttribute.class.isInstance(attribute)) {
					map = comments;
				} else if (CharacteristicAttribute.class.isInstance(attribute)) {
					map = characteristics;
				}
				if (map != null) {
					String key = attribute.getAttributeType();
					String value = attribute.getAttributeValue();
					if (!map.containsKey(key)) {
						map.put(key,
								Collections
										.synchronizedMap(new HashMap<String, Integer>()));
					}
					if (map.get(key).containsKey(value)) {
						int count = map.get(key).get(value).intValue();
						map.get(key).put(value,
								count + 1);
					} else {
						map.get(key).put(value, new Integer(1));
					}
				}
			}
		}
	}

	public static void main(String[] args) {

	}

}
