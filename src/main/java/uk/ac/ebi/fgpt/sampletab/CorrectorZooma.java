package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class CorrectorZooma {
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());
    
    LoadingCache<String, String> lookup = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
        new CacheLoader<String, String>() {
          public String load(String query) throws JsonParseException, JsonMappingException, IOException {
            return getZoomaHit(query);
          }
        });

    private String getZoomaHit(String query) throws JsonParseException, JsonMappingException, IOException {
        //URL jsurl = new URL("http://megatron.windows.ebi.ac.uk:8080/zooma/v2/api/search?query="+URLEncoder.encode(attr.getAttributeValue(), "UTF-8"));
        URL jsurl = new URL("http://wwwdev.ebi.ac.uk/fgpt/zooma/v2/api/search?query="+URLEncoder.encode(query, "UTF-8"));
        log.debug("URL "+jsurl.toExternalForm());
        
        ObjectMapper mapper = new ObjectMapper();                                                        
        JsonNode rootNode = mapper.readValue(jsurl, JsonNode.class); // src can be a File, URL, InputStream etc

        log.debug(rootNode.toString());
        
        JsonNode results = rootNode.get("result");
        if (results != null){
            JsonNode tophit = results.get(0);
            if (tophit != null){
                String fixName = tophit.get("name").getTextValue();
                Float score = new Float(tophit.get("score").getTextValue());
                String fixTermSourceID;
                String fixTermSourceREF;
                log.info(query+" -> "+fixName+" ("+score+")");
                return fixName;
            }
        }  
        return "";
    }
    
    public void correct(SampleData sampledata) {        
        for (SampleNode s : sampledata.scd.getNodes(SampleNode.class)) {
            for (SCDNodeAttribute a : new ArrayList<SCDNodeAttribute>(s.getAttributes())) {
                boolean isAbstractNodeAttributeOntology = false;
                synchronized(AbstractNodeAttributeOntology.class){
                    isAbstractNodeAttributeOntology = AbstractNodeAttributeOntology.class.isInstance(a);
                }
                
                if (isAbstractNodeAttributeOntology){
                    try {
                        AbstractNodeAttributeOntology attr = (AbstractNodeAttributeOntology) a;

                        String value = attr.getAttributeValue().trim();
                        
                        if (value.matches("[0-9.]+")){
                            //do nothing
                        } else {
                        
                            try {
                                lookup.get(value);
                            } catch (ExecutionException e) {
                                throw e.getCause();
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        log.error("Error processing "+a, e);
                        return;
                    } catch (JsonParseException e) {
                        log.error("Error processing "+a, e);
                        return;
                    } catch (JsonMappingException e) {
                        log.error("Error processing "+a, e);
                        return;
                    } catch (IOException e) {
                        log.error("Error processing "+a, e);
                        return;
                    } catch (Throwable e) {
                        log.error("Error processing "+a, e);
                        return;
                    }
                }
                
            }
        }
    }
}
