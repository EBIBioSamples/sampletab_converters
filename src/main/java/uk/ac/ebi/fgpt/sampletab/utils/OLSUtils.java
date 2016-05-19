package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class OLSUtils {

    private static Logger log = LoggerFactory.getLogger(ENAUtils.class);

	private static RestTemplate restTemplate = new RestTemplate();

	private static ObjectMapper mapper = new ObjectMapper();
	
	
	public static URI guessIRIfromShortTerm(String shortTerm) throws IOException, URISyntaxException, TooManyIRIsException {
		String queryUrl = "https://www.ebi.ac.uk/ols/api/terms?short_form="+shortTerm;
		ResponseEntity<String> response = restTemplate.getForEntity(queryUrl, String.class);
		if (!response.getStatusCode().equals(HttpStatus.OK) ) {
			throw new IOException("Problem getting HTTP response "+response.getStatusCode());
		}
		
		JsonNode root = mapper.readTree(response.getBody());
		log.trace("root "+root);
		log.trace("_embedded "+root.path("_embedded"));
		log.trace("terms "+root.path("_embedded").path("terms"));
		
		if (!root.path("_embedded").path("terms").isContainerNode()) {
			throw new IOException("Problem with HTTP response");
		}
		
		Set<URI> iris = new HashSet<>();
		Iterator<JsonNode> termIterator = root.path("_embedded").path("terms").getElements();
		while (termIterator.hasNext()) {
			JsonNode term = termIterator.next();
			iris.add(new URI(term.path("iri").asText()));
		}
		
		if (iris.size() == 1) {
			return iris.iterator().next();
		} else {
			throw new TooManyIRIsException(""+shortTerm+" has "+iris.size()+" IRIs");
		}
	}
	
	public static class TooManyIRIsException extends Exception {

		private static final long serialVersionUID = -8291453743076363740L;

		public TooManyIRIsException(String string) {
			super(string);
		}
		
	}
}
