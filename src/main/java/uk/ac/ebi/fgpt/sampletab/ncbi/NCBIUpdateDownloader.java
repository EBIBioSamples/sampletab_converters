package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

/**
 * This tool downloads all the sample information from NCBI
 * biosamples based on the modification and publication date 
 * @author drashtti 
 * @author faulcon 
 */
public class NCBIUpdateDownloader {
    
	private String base = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/";	

    private SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/dd");

    private Logger log = LoggerFactory.getLogger(getClass());
        
    public Collection<Integer> getUpdatedSampleIds(Date from, Date to) throws MalformedURLException, DocumentException, IOException {
        
        Collection<Integer> ids = new HashSet<Integer>();
        
        //construct a query that the ncbi search engine understands
        String query = dateFormat.format(from)+":"+dateFormat.format(to)+"[MDAT]+OR+"
            +dateFormat.format(from)+":"+dateFormat.format(to)+"[PDAT]+AND+public[Filter]";
        
        int retrievalSize = 100;
        int retrievalOffset = 0;
        int count = 100;
        int retMax = 0;
        int retStart = 0;
        while (retStart < count) {

            //get the results from ncbi via eSearch
            //make sure it is from BioSamples
            //make sure the search term is URL encoded
            //get it in chunks to handle large results better
            //String url = base + "esearch.fcgi?db=biosample&term=" + URLEncoder.encode(query, "UTF-8")
            String url = base + "esearch.fcgi?db=biosample&term=" + query
                + "&retmax="+retrievalSize
                + "&retstart="+retrievalOffset;
            
            log.info(""+count+" "+retMax+" "+retStart);
            log.info("Getting url "+url);
            
            Document results = XMLUtils.getDocument(new URL(url));
            Element root = results.getRootElement();
            
            retMax = Integer.parseInt(XMLUtils.getChildByName(root, "RetMax").getTextTrim());
            retStart = Integer.parseInt(XMLUtils.getChildByName(root, "RetStart").getTextTrim());
            count = Integer.parseInt(XMLUtils.getChildByName(root, "Count").getTextTrim());
            
            Element idList = XMLUtils.getChildByName(root, "IdList");
            
            for (Element id : XMLUtils.getChildrenByName(idList, "Id")) {
                ids.add(Integer.parseInt(id.getTextTrim()));
            }
            
            retrievalOffset += retrievalSize;
        }
        
        return ids;
    }
    
    public Document getId(int id) throws MalformedURLException, DocumentException, IOException {

        String url = base + "efetch.fcgi?db=biosample" 
            + "&retmode=xml&rettype=full"
            + "&id=" + id;

        log.info("Getting url "+url);
        
        Document result = XMLUtils.getDocument(new URL(url));
        return result;
    }
}
