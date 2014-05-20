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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
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
    
	private static String base = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/";	


    private static Logger log = LoggerFactory.getLogger(NCBIUpdateDownloader.class);
        
    public static Collection<Integer> getUpdatedSampleIds(Date from, Date to) throws MalformedURLException, DocumentException, IOException {
        
        Collection<Integer> ids = new HashSet<Integer>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/dd");
        
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
    
    public static Document getId(int id) throws MalformedURLException, DocumentException, IOException {

        String url = base + "efetch.fcgi?db=biosample" 
            + "&retmode=xml&rettype=full"
            + "&id=" + id;

        log.info("Getting url "+url);
        
        Document result = XMLUtils.getDocument(new URL(url));
        return result;
    }
    
    /**
     * Wrapper around callable suitable for use in multithreaded applications
     * 
     * Note: this stores the document in memory, when using large numbers of callables
     * this may become significant.
     * 
     * @author faulcon
     *
     */
    public static class UpdateCallable implements Callable<Document> {

        private final Integer id;
        
        public UpdateCallable(int id) {
            this.id = id;
        }
        
        @Override
        public Document call() throws Exception {
            return getId(id);            
        }

    }
    

    /**
     * Custom iterable that creates an @NCBIUpdateIterator
     * 
     * @author faulcon
     *
     */
    public static class UpdateIterable implements Iterable<Integer> {

        private final Date from;
        private final Date to;
        
        public UpdateIterable(Date from, Date to) {
            this.from = from;
            this.to = to;
        }
        
        @Override
        public Iterator<Integer> iterator() {
            return new NCBIUpdateIterator(from, to);
        }
        
    }
    
    /**
     * Custom iterator that fetches updated samples on demand. Improves performance
     * by allowing processing of early results before all results are gathered.
     * 
     * @author faulcon
     *
     */
    protected static class NCBIUpdateIterator implements Iterator<Integer> {

        private LinkedList<Integer> buffer = new LinkedList<Integer>();

        private SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/dd");
        
        private int retrievalSize = 100;
        private int retrievalOffset = 0;
        private int count = 100;
        private int retMax = 0;
        private int retStart = 0;
        private final String query;
        
        
        public NCBIUpdateIterator(Date from, Date to) {

            //construct a query that the ncbi search engine understands
            query = dateFormat.format(from)+":"+dateFormat.format(to)+"[MDAT]+OR+"
                +dateFormat.format(from)+":"+dateFormat.format(to)+"[PDAT]+AND+public[Filter]";
        }
        
        @Override
        public boolean hasNext() {
            if (buffer.size() > 0) {
                return true;
            }
            if (retStart < count) {
                //get the next batch

                //get the results from ncbi via eSearch
                //make sure it is from BioSamples
                //get it in chunks to handle large results better
                //no need to URL encode the query
                String url = base + "esearch.fcgi?db=biosample&term=" + query
                    + "&retmax="+retrievalSize
                    + "&retstart="+retrievalOffset;
                
                log.info(""+count+" "+retMax+" "+retStart);
                log.info("Getting url "+url);
                
                Document results;
                try {
                    results = XMLUtils.getDocument(new URL(url));
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                } catch (DocumentException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Element root = results.getRootElement();
                
                retMax = Integer.parseInt(XMLUtils.getChildByName(root, "RetMax").getTextTrim());
                retStart = Integer.parseInt(XMLUtils.getChildByName(root, "RetStart").getTextTrim());
                count = Integer.parseInt(XMLUtils.getChildByName(root, "Count").getTextTrim());
                
                Element idList = XMLUtils.getChildByName(root, "IdList");
                
                for (Element id : XMLUtils.getChildrenByName(idList, "Id")) {
                    buffer.add(Integer.parseInt(id.getTextTrim()));
                }
                
                retrievalOffset += retrievalSize;
            }
            if (buffer.size() > 0) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Integer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.pop();
        }

        @Override
        public void remove() {
            //do nothing
        }
        
    }
}
