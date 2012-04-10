package uk.ac.ebi.fgpt.sampletab.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;

public class TaxonUtils {
    
    public static  String getTaononOfID(int taxID) throws DocumentException{
        //TODO add meta information identifying this tool
        //TODO add caching
        String URL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=taxonomy&id="+taxID;
        Document doc = XMLUtils.getDocument(URL);
        Element root = doc.getRootElement();
        Element docsum = XMLUtils.getChildByName(root, "DocSum");
        for (Element item : XMLUtils.getChildrenByName(docsum, "Item")){
            if ("ScientificName".equals(item.attributeValue("Name"))){
                return item.getTextTrim();
            }
        }
        return null;
    }
}
