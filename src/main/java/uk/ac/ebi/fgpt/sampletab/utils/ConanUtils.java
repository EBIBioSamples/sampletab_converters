package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConanUtils {
    private final static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.utils.ConanUtils");
    
    private static Properties properties = null;
    
    private static PoolingHttpClientConnectionManager conman = null;
    private static CloseableHttpClient httpClient = null;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private synchronized static void setup(){
        if (properties == null){
            properties = new Properties();
            try {
                InputStream is = ConanUtils.class.getResourceAsStream("/sampletabconverters.properties");
                properties.load(is);
            } catch (IOException e) {
                log.error("Unable to read resource sampletabconverters.properties", e);
            }
        }
        if (conman == null) {
        	conman = new PoolingHttpClientConnectionManager();
        	conman.setDefaultMaxPerRoute(10);
        	conman.setValidateAfterInactivity(0);
        	httpClient = HttpClients.custom().setConnectionManager(conman).build();
        }
    }

    public static void submit(String submissionIdentifier, String pipeline) throws IOException{
        submit(submissionIdentifier, pipeline, 0);
    }
    
    public static void submit(String submissionIdentifier, String pipeline, int startingProcessIndex) throws IOException{
        setup();
        
        ObjectNode userOb = objectMapper.createObjectNode();
        
        userOb.put("priority", properties.getProperty("biosamples.conan.priority"));
        userOb.put("pipelineName", pipeline);
        userOb.put("startingProcessIndex", startingProcessIndex);
        userOb.put("restApiKey", properties.getProperty("biosamples.conan.apikey"));
        ObjectNode inputParameters = userOb.putObject("inputParameters");
        inputParameters.put("SampleTab Accession", submissionIdentifier);
        
        log.trace(userOb.toString());

        // Send data
        HttpPost postRequest = new HttpPost(properties.getProperty("biosamples.conan.url")+"/api/submissions/");
        postRequest.setConfig(RequestConfig.custom()
        	    .setSocketTimeout(0)
        	    .setConnectTimeout(0)
        	    .setConnectionRequestTimeout(0)
        	    .build());
        StringEntity input = new StringEntity(userOb.toString());
        input.setContentType("application/json");
        postRequest.setEntity(input);
 
        //get response
        try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
	        try (BufferedReader br = new BufferedReader(
	                new InputStreamReader((response.getEntity().getContent())))) {
	            //TODO parse response and raise exception if submit failed
		        String line;
		        while ((line = br.readLine()) != null) {
		            log.info(line);
		        }
	        }
        }
    }
}
