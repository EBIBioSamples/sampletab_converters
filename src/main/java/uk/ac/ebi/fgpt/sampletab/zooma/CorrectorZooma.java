package uk.ac.ebi.fgpt.sampletab.zooma;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CorrectorZooma {
    
    // logging
    private static Logger log = LoggerFactory.getLogger(uk.ac.ebi.fgpt.sampletab.zooma.CorrectorZooma.class);
    
    public static LoadingCache<String, JsonNode> lookupString = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
        new CacheLoader<String, JsonNode>() {
          public JsonNode load(String query) throws JsonParseException, JsonMappingException, IOException {
            return getZoomaStringHit(query);
          }
        });

    public static JsonNode getZoomaStringHit(String query) throws JsonParseException, JsonMappingException, IOException {
        query = URLEncoder.encode(query, "UTF-8");
        URL jsurl = new URL("http://wwwdev.ebi.ac.uk/fgpt/zooma/v2/api/search?query="+query);
        log.debug("URL "+jsurl.toExternalForm());
        
        ObjectMapper mapper = new ObjectMapper();                                                        
        JsonNode rootNode = mapper.readValue(jsurl, JsonNode.class); // src can be a File, URL, InputStream etc

        log.debug(rootNode.toString());
        
        JsonNode results = rootNode.get("result");
        if (results != null){
            JsonNode tophit = results.get(0);
            return tophit;
        }  
        return null;
    }
    
    public static LoadingCache<String[], JsonNode> lookupKeyValue = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
        new CacheLoader<String[], JsonNode>() {
          public JsonNode load(String[] query) throws JsonParseException, JsonMappingException, IOException {
            return getZoomaKeyValueHit(query);
          }
        });

    public static JsonNode getZoomaKeyValueHit(String[] query) throws JsonParseException, JsonMappingException, IOException {
        return getZoomaKeyValueHit(query[0], query[1]);
    }
    
    public static JsonNode getZoomaKeyValueHit(String key, String value) throws JsonParseException, JsonMappingException, IOException {
        key = URLEncoder.encode(key, "UTF-8");
        value = URLEncoder.encode(value, "UTF-8");
        URL jsurl = new URL("http://wwwdev.ebi.ac.uk/fgpt/zooma/v2/api/search?query="+value+"&type="+key);
        log.debug("URL "+jsurl.toExternalForm());
        
        ObjectMapper mapper = new ObjectMapper();                                                        
        JsonNode rootNode = mapper.readValue(jsurl, JsonNode.class); // src can be a File, URL, InputStream etc

        log.debug(rootNode.toString());
        
        JsonNode results = rootNode.get("result");
        if (results != null){
            JsonNode tophit = results.get(0);
            return tophit;
        }  
        return null;
    }
    
    
    public static void correct(SampleData sampledata) {        
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
                            JsonNode tophit = null;
                            try {
                                tophit = lookupString.get(value);
                            } catch (ExecutionException e) {
                                throw e.getCause();
                            }
                            if (tophit != null){
                                String fixName = tophit.get("name").getTextValue();
                                Float score = new Float(tophit.get("score").getTextValue());
                                String fixTermSourceID;
                                String fixTermSourceREF;
                                //TODO finish
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
