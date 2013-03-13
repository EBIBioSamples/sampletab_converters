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

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CorrectorZooma {
    
    // logging
    private static Logger log = LoggerFactory.getLogger(uk.ac.ebi.fgpt.sampletab.zooma.CorrectorZooma.class);
    
    private static LoadingCache<String[], Optional<JsonNode>> lookupString = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
        new CacheLoader<String[], Optional<JsonNode>>() {
          public Optional<JsonNode> load(String[] query) throws JsonParseException, JsonMappingException, IOException {
              String key = query[0];
              String value = query[1];
              URL jsurl = null;
              value = URLEncoder.encode(value, "UTF-8");
              if (key != null){
                  key = URLEncoder.encode(value, "UTF-8");
                  jsurl = new URL("http://www.ebi.ac.uk/fgpt/zooma/v2/api/search?query="+value+"&type="+key);
              } else {
                  jsurl = new URL("http://www.ebi.ac.uk/fgpt/zooma/v2/api/search?query="+value);
              }
              log.debug("URL "+jsurl.toExternalForm());
              
              ObjectMapper mapper = new ObjectMapper();                                                                                                           
              JsonNode rootNode = null;
              try{
                  rootNode = mapper.readValue(jsurl, JsonNode.class); // src can be a File, URL, InputStream etc
              } catch (IOException e) {
                  if (e.getMessage().contains("Server returned HTTP response code: 500 for URL")){
                      return Optional.absent();
                  } else {
                      throw e;
                  }
              }

              log.debug(rootNode.toString());
              
              JsonNode results = rootNode.get("result");
              Optional<JsonNode> toReturn = Optional.fromNullable(results); 
              return toReturn;
          }
        });
    
    
    public static String getTopStringOfValueQuery(String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        return getTopNodeOfValueQuery(value).get("name").getTextValue();
    }
    
    public static JsonNode getTopNodeOfValueQuery(String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        return getTopNodeOfKeyValueQuery(null, value);
    }
    
    public static String getTopStringOfKeyValueQuery(String key, String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        return getTopNodeOfKeyValueQuery(key, value).get("name").getTextValue();
    }
    
    public static JsonNode getTopNodeOfKeyValueQuery(String key, String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        return getAllNodesOfKeyValueQuery(key, value).get(0);
    }

    public static JsonNode getAllNodesOfValueQuery(String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        return getAllNodesOfKeyValueQuery(null, value);
    }
    
    public static JsonNode getAllNodesOfKeyValueQuery(String key, String value) throws ExecutionException, JsonParseException, JsonMappingException, IOException {
        String[] query = new String[2];
        query[0] = key;
        query[1] = value;
        Optional<JsonNode> returned = null;
        try {
             returned = lookupString.get(query);
        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (JsonParseException e2) {
                throw e2;
            } catch(JsonMappingException e2) {
                throw e2;
            } catch (IOException e2) {
                throw e2;
            } catch (Throwable e2) {
                log.error("Unexpected exception", e2);
                throw e;
            }
        }
        if (returned == null){
            return null;
        } else{
            return returned.orNull();
        }
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
                                tophit = getTopNodeOfValueQuery(value);
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
