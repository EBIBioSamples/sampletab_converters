package uk.ac.ebi.fgpt.sampletab.utils;

import java.util.HashSet;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;


public class BioSDUtils {

    public static Set<String> getBioSDAccessionOf(String databaseID) throws DocumentException{
        //returns biosamples accession(s) given a source database name and database id
        Set<String> accessions  = new HashSet<String>();
        
        //get matched group
        Document querydoc = XMLUtils.getDocument("http://www.ebi.ac.uk/biosamples/xml/group/query="+databaseID);
        Element queryel = querydoc.getRootElement();
        for (Element resultgroup : XMLUtils.getChildrenByName(queryel, "BioSampleGroup")){
            String groupid = resultgroup.attributeValue("id");
            //get matched samples in that group
            Document groupdoc = XMLUtils.getDocument("http://www.ebi.ac.uk/biosamples/xml/groupsamples/"+groupid+"/query="+databaseID);
            Element groupel = groupdoc.getRootElement();
            for (Element resultsample : XMLUtils.getChildrenByName(groupel, "BioSample")){
                String sampleid = resultsample.attributeValue("id");
                //double-check each sample
                Document sampledoc = XMLUtils.getDocument("http://www.ebi.ac.uk/biosamples/xml/sample/"+sampleid);
                Element sampleel = sampledoc.getRootElement();
                for (Element propertyel : XMLUtils.getChildrenByName(sampleel, "Property")){
                    if (propertyel.attributeValue("class").equals("Database ID")){
                        String value = XMLUtils.getChildByName(propertyel, "Value").getTextTrim();
                        if (value.equals(databaseID)){
                            accessions.add(sampleid);
                        }
                    }
                }
            }
        }
        
        return accessions;
    }
}
