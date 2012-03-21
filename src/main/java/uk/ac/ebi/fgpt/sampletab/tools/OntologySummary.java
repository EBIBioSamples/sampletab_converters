package uk.ac.ebi.fgpt.sampletab.tools;

import java.util.Collections;
import java.util.HashMap;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNamedAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class OntologySummary extends AttributeSummary {

    
    protected void processAttribute(SCDNodeAttribute attribute){
        String key = null;
        String value = null;
        
        if (AbstractNodeAttributeOntology.class.isInstance(attribute)){
            AbstractNodeAttributeOntology a = (AbstractNodeAttributeOntology) attribute;
            key = a.getTermSourceREF();
            value = a.getTermSourceID();
        }
        
        if (key != null && value != null){
            if (!attributes.containsKey(key)) {
                attributes.put(key,
                        Collections
                                .synchronizedMap(new HashMap<String, Integer>()));
            }
            if (attributes.get(key).containsKey(value)) {
                int count = attributes.get(key).get(value).intValue();
                attributes.get(key).put(value,
                        count + 1);
            } else {
                attributes.get(key).put(value, new Integer(1));
            }
        }
    }
    
    public static void main(String[] args) {
        new OntologySummary().doMain(args);
    }

}
