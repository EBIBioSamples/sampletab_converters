package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class ENAUtils {

	
	public String getStudyForSample(String srsId) throws SAXException, IOException, ParserConfigurationException{
		String url = "http://www.ebi.ac.uk/ena/data/view/"+srsId+"&display=xml";	
		DocumentBuilderFactory docbuilderfactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docbuilder = docbuilderfactory.newDocumentBuilder();
		Document doc = docbuilder.parse(new URL(url).openStream());
		
		Element root = doc.getDocumentElement();
		Element links = XMLUtils.getChildByName(root, "SAMPLE_LINKS");
		for (Element link : XMLUtils.getChildrenByName(links, "SAMPLE_LINK")){
			Element xref =  XMLUtils.getChildByName(root, "XREF_LINK");
			if (xref != null){
				Element db = XMLUtils.getChildByName(xref, "DB");
				Element id = XMLUtils.getChildByName(xref, "ID");
				if (db != null && db.getTextContent()=="ENA-STUDY" && id != null) {
					//TODO assumes only one study per sample. 
					return id.getTextContent();
				}
			}
		}
		// did not mach, return null;
		return null;
	}
}
