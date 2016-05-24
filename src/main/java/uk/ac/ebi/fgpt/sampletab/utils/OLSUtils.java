package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class OLSUtils {

	private static Logger log = LoggerFactory.getLogger(OLSUtils.class);

	private static RestTemplate restTemplate = new RestTemplate();
	
	//make the rest template use a pool of http connections
	static{
    	PoolingHttpClientConnectionManager conman = new PoolingHttpClientConnectionManager();
    	conman.setMaxTotal(128);
    	conman.setDefaultMaxPerRoute(64);
    	conman.setValidateAfterInactivity(0);
    	
    	ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            	//see if the user provides a live time
                HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        return Long.parseLong(value) * 1000;
                    }
                }
                //default to one second live time 
                return 1 * 1000;
            }
        };
    	
    	CloseableHttpClient httpClient = HttpClients.custom()
    			.setKeepAliveStrategy(keepAliveStrategy)
    			.setConnectionManager(conman).build();
    	
		restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(httpClient));
	}

	private static ObjectMapper mapper = new ObjectMapper();

	private static LoadingCache<String, Optional<URI>> guessedURIs = CacheBuilder.newBuilder()
		.maximumSize(1000)
		.build(new CacheLoader<String, Optional<URI>>() {
			public Optional<URI> load(String shortTerm) throws IOException, URISyntaxException, TooManyIRIsException {
				String queryUrl = "https://www.ebi.ac.uk/ols/api/terms?short_form=" + shortTerm;
				ResponseEntity<String> response = restTemplate.getForEntity(queryUrl, String.class);
				if (!response.getStatusCode().equals(HttpStatus.OK)) {
					throw new IOException("Problem getting HTTP response " + response.getStatusCode());
				}
	
				JsonNode root = mapper.readTree(response.getBody());
				log.trace("root " + root);
				log.trace("_embedded " + root.path("_embedded"));
				log.trace("terms " + root.path("_embedded").path("terms"));
	
				if (!root.path("_embedded").path("terms").isContainerNode()) {
					//no matches
					return Optional.empty();
				}
	
				Set<URI> iris = new HashSet<>();
				Iterator<JsonNode> termIterator = root.path("_embedded").path("terms").getElements();
				while (termIterator.hasNext()) {
					JsonNode term = termIterator.next();
					iris.add(new URI(term.path("iri").asText()));
				}
	
				if (iris.size() == 1) {
					return Optional.ofNullable(iris.iterator().next());
				} else {
					throw new TooManyIRIsException("" + shortTerm + " has " + iris.size() + " IRIs");
				}
			}
		});

	public static Optional<URI> guessIRIfromShortTerm(String shortTerm)
			throws IOException, URISyntaxException, TooManyIRIsException {
		try {
			return guessedURIs.get(shortTerm);
		} catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (IOException e2) {
                throw e2;
            } catch (URISyntaxException e2) {
                throw e2;
            } catch (TooManyIRIsException e2) {
                throw e2;
            } catch (Throwable e2) {
                throw new RuntimeException("Unrecognised ExecutionException", e2);
            }
		}
	}

	public static class TooManyIRIsException extends Exception {

		private static final long serialVersionUID = -8291453743076363740L;

		public TooManyIRIsException(String string) {
			super(string);
		}

	}
}
