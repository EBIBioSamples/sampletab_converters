package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.net.URL;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;


public class EuroPMCUtils {
    
    
    public static String getTitleByPUBMEDid(Integer pubmed) throws DocumentException, IOException {
        URL url = new URL("http://www.ebi.ac.uk/europepmc/webservices/rest/search/query=ext_id:"+pubmed);
        Document doc =  XMLUtils.getDocument(url);
        
        Element root = doc.getRootElement();
        if (root == null) throw new DocumentException("root is null");
        
        Element resultList = XMLUtils.getChildByName(root, "resultList");
        if (resultList == null) throw new DocumentException("resultList is null");
        
        Element result = XMLUtils.getChildByName(resultList, "result");
        if (result == null) throw new DocumentException("result is null");
        
        Element title = XMLUtils.getChildByName(result, "title");
        if (title == null) throw new DocumentException("title is null");
        
        return title.getTextTrim();
        
    }
    
}
