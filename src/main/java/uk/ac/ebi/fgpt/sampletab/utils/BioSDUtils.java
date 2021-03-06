package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;


public class BioSDUtils {

    public static Set<String> getBioSDAccessionOf(String databaseID) throws DocumentException, IOException {
        //returns biosamples accession(s) given a source database name and database id
        Set<String> accessions  = new HashSet<String>();
        
        //get matched group
        URL queryURL = new URL("http://www.ebi.ac.uk/biosamples/xml/group/query="+databaseID);
        Document querydoc = XMLUtils.getDocument(queryURL);
        Element queryel = querydoc.getRootElement();
        for (Element resultgroup : XMLUtils.getChildrenByName(queryel, "BioSampleGroup")){
            String groupid = resultgroup.attributeValue("id");
            //get matched samples in that group
            URL groupURL = new URL("http://www.ebi.ac.uk/biosamples/xml/groupsamples/"+groupid+"/query="+databaseID);
            Document groupdoc = XMLUtils.getDocument(groupURL);
            Element groupel = groupdoc.getRootElement();
            for (Element resultsample : XMLUtils.getChildrenByName(groupel, "BioSample")){
                String sampleid = resultsample.attributeValue("id");
                //double-check each sample
                URL sampleURL = new URL("http://www.ebi.ac.uk/biosamples/xml/sample/"+sampleid);
                Document sampledoc = XMLUtils.getDocument(sampleURL);
                Element sampleel = sampledoc.getRootElement();
                for (Element propertyel : XMLUtils.getChildrenByName(sampleel, "Property")){
                    //TODO check this is still valid
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
    
    public static boolean isBioSDAccessionPublic(String accession) {
        try {
            URL queryURL = new URL("http://www.ebi.ac.uk/biosamples/xml/sample/"+accession);
            Document querydoc = XMLUtils.getDocument(queryURL);
            if (querydoc.getRootElement() == null) {
            	return false;
            } else {
            	return true;
            }
            //don't need to do anything with the doc, just see if it is accessible
        } catch (MalformedURLException e) {
            return false;
        } catch (DocumentException e) {
            return false;
        } catch (FileNotFoundException e) {
            //if the URL does not exist, then the website throws a 404 error
            //java interprets this as a filenotfoundexception
            return false;
        } catch (IOException e) {
            return false;
        }
    }
    
}
