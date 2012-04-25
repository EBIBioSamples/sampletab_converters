package uk.ac.ebi.fgpt.sampletab.tools;

import java.util.Collections;
import java.util.HashMap;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNamedAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class UnitSummary extends AttributeSummary {

    
    protected void processAttribute(SCDNodeAttribute attribute){
        String key = null;
        String value = null;
        
        AbstractNodeAttributeOntology unit = null;
        
        if (CommentAttribute.class.isInstance(attribute)){
            CommentAttribute a = (CommentAttribute) attribute;
            unit = a.unit;
        } else if (CharacteristicAttribute.class.isInstance(attribute)){
            CharacteristicAttribute a = (CharacteristicAttribute) attribute;
            unit = a.unit;
        }
        
        if (unit != null){
            key = "unit";
            value = unit.getAttributeValue();
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
        new UnitSummary().doMain(args);
    }

}
