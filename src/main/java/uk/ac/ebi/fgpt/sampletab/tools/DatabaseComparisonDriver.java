package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;

import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.jolbox.bonecp.BoneCPDataSource;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.ncbi.NCBIFTP;
import uk.ac.ebi.fgpt.sampletab.utils.XMLFragmenter;
import uk.ac.ebi.fgpt.sampletab.utils.XMLFragmenter.ElementCallback;

public class DatabaseComparisonDriver extends AbstractDriver {

    private Logger log = LoggerFactory.getLogger(getClass());
	public DatabaseComparisonDriver() {
		
	}


	public static void main(String[] args) {
		new DatabaseComparisonDriver().doMain(args);
	}
	
    @Override
    public void doMain(String[] args){
        super.doMain(args);
        
        //get the SAME accessions in biosamples
        log.info("Getting SAME accessions in BioSamples");
        Set<String> sameBio = getBioSampleSAME();
        output(sameBio, new File("sameBio.txt"));
        log.info("Got SAME accessions in biosamples ("+sameBio.size()+")");
        
        //get the SAME accessions public in ENA 
        log.info("Getting SAME accessions public in ENA");
        Set<String> sameEna = getENASAME();
        output(sameEna, new File("sameEna.txt"));
        log.info("Got SAME accessions public in ENA ("+sameEna.size()+")");
        
        //find SAME accessions in ENA but not BioSamples
        log.info("Getting SAME accessions in Ena but not BioSamples");
        Set<String> sameEnaNotBio = getInANotB(sameEna, sameBio);
        output(sameEnaNotBio, new File("sameEnaNotBio.txt"));
        log.info("Got SAME accessions in ENA but not BioSamples ("+sameEnaNotBio.size()+")");
        
        //find submissions for SAME accessions in ENA but not BioSamples
        log.info("Getting ERA submissions for samples in Ena but not BioSamples");
        Set<String> enaSubmissions = getENASubmissionsForSAMEAccessions(sameEnaNotBio);
        output(enaSubmissions, new File("eraEnaNotBio.txt"));
        log.info("Got ERA submissions for samples in Ena but not BioSamples ("+enaSubmissions.size()+")");
        
        // get the SAMN/D accession in biosamples
        log.info("Getting SAMN accessions in BioSamples");
        Set<String> samnBio = getBioSampleSAMN();
        output(samnBio, new File("samnBio.txt"));
        log.info("Got SAMN accessions in BioSamples ("+samnBio.size()+")");
        
        // get the SAMN/D accession public in ENA
        log.info("Getting SAMN accessions public in ENA");
        Set<String> samnEna = getENASAMN();
        output(samnEna, new File("samnEna.txt"));
        log.info("Got SAMN accessions public in ENA ("+samnEna.size()+")");
        
        // get the SAMN/D accession public in NCBI
        log.info("Getting SAMN accessions public in NCBI");
        Map<String, Integer> samnNcbi = getNCBISAMN();
        output(samnNcbi.keySet(), new File("samnNcbi.txt"));
        log.info("Got SAMN accessions public in NCBI ("+samnNcbi.size()+")");
        
        //find SAMN/D accession public in NCBI but not biosamples
        log.info("Getting SAMN accessions in NCBI but not BioSamples");
        Set<String> samnNcbiNotBio = getInANotB(samnNcbi.keySet(), samnBio);
        output(samnNcbiNotBio, new File("samnNcbiNotBio.txt"));
        log.info("Got SAMN accessions in NCBI but not BioSamples ("+samnNcbiNotBio.size()+")");
        
        //find NCBI IDs for SAMN/D accession public in NCBI but not biosamples
        log.info("Getting NCBI IDs in NCBI but not BioSamples");
        Set<String> idNcbiNotBio = getNCBIIDforAccession(samnNcbiNotBio, samnNcbi);
        output(idNcbiNotBio, new File("idNcbiNotBio.txt"));
        log.info("Got NCBI IDs in NCBI but not BioSamples ("+idNcbiNotBio.size()+")");
        
        //find SAMN/D accession public ENA but not NCBI
        log.info("Getting SAME accessions in ENA but not NCBI");
        Set<String> samnEnaNotNcbi = getInANotB(samnEna, samnNcbi.keySet());
        output(samnEnaNotNcbi, new File("samnEnaNotNcbi.txt"));
        log.info("Got SAME accessions in ENA but not NCBI ("+samnEnaNotNcbi.size()+")");
    }
    
    private void output(Set<String> in, File file) {
    	List<String> sorted = new ArrayList<String>(in);
    	Collections.sort(sorted);
    	Writer out = null;
    	try {
			out = new BufferedWriter(new FileWriter(file));
	    	for (String acc : sorted) {
	    		out.append(acc);
	    		out.append("\n");
	    	}
    	} catch (IOException e) {
    		throw new RuntimeException(e);
		} finally {
    		if (out != null) {
    	    	try {
					out.close();
				} catch (IOException e) {
					//do nothing
				}		
    		}
    	}
    }
    
    private Set<String> getInANotB(Set<String> a, Set<String> b) {
        Set<String> out = new HashSet<String>();
        for (String acc : a) {
        	if (!b.contains(acc)) {
        		out.add(acc);
        	}
        }
        return out;
    }
    
    private DataSource getBioSampleDataSource() {
        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl("jdbc:oracle:thin:@ora-vm-023.ebi.ac.uk:1531:biosdpro");
        ds.setUser("biosd");
        ds.setPassword("b10sdp40");  
        return ds;
    }
    
    private JdbcTemplate getBioSampleJdbcTemplate() {
    	JdbcTemplate t = new JdbcTemplate();
    	t.setDataSource(getBioSampleDataSource());
    	return t;
    }
    
    private RowMapper<String> getAccessionRowMapper() {        
    	RowMapper<String> rm = new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString(1);
			}
    	};
    	return rm;    	
    }
    
    private Set<String> getBioSampleSAME() {
    	Set<String> acc = new HashSet<String>();
    	acc.addAll(getBioSampleJdbcTemplate().query("SELECT ACC FROM BIO_PRODUCT WHERE ACC LIKE 'SAME%'", getAccessionRowMapper()));    	
    	return acc;
    }
    
    private Set<String> getBioSampleSAMN() {
    	Set<String> acc = new HashSet<String>();
    	acc.addAll(getBioSampleJdbcTemplate().query("SELECT ACC FROM BIO_PRODUCT WHERE ACC LIKE 'SAM%' AND ACC NOT LIKE 'SAME%'", getAccessionRowMapper()));    	
    	return acc;
    }
    
    private DataSource getENADataSource() {
        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl("jdbc:oracle:thin:@ora-vm-009.ebi.ac.uk:1541:erapro");
        ds.setUser("era_reader");
        ds.setPassword("reader");  
        return ds;
    }

    private JdbcTemplate getENAJdbcTemplate() {
    	JdbcTemplate t = new JdbcTemplate();
    	t.setDataSource(getENADataSource());
    	return t;
    }
    
    private Set<String> getENASAME() {
    	Set<String> acc = new HashSet<String>();
    	acc.addAll(
    			getENAJdbcTemplate().query(
    					"SELECT BIOSAMPLE_ID FROM SAMPLE WHERE BIOSAMPLE_ID LIKE 'SAME%' "
    							+ "AND STATUS_ID = 4 AND EGA_ID IS NULL", 
    					getAccessionRowMapper()));    	
    	return acc;
    }
    
    private Set<String> getENASAMN() {
    	Set<String> acc = new HashSet<String>();
    	acc.addAll(
    			getENAJdbcTemplate().query(
    					"SELECT BIOSAMPLE_ID FROM SAMPLE WHERE BIOSAMPLE_ID LIKE 'SAM%' AND BIOSAMPLE_ID NOT LIKE 'SAME%' "
    							+ "AND STATUS_ID = 4 AND EGA_ID IS NULL", 
    					getAccessionRowMapper()));    	
    	return acc;
    }

    private Map<String, Integer> getNCBISAMN() {

		final Map<String, Integer> accessions = new HashMap<String, Integer>();

    	ElementCallback callback = new ElementCallback() {
    		
    		@Override
    		public void handleElement(Element e) {
    			String acc = e.attributeValue("accession");
    			Integer id = Integer.parseInt(e.attributeValue("id"));
    			accessions.put(acc, id);
    		}

    		@Override
    		public boolean isBlockStart(String uri, String localName, String qName,
    				Attributes attributes) {
    			//its not a biosample element, skip
    			if (!qName.equals("BioSample")) {
    				return false;
    			}
    			//its not public, skip
    			if (!attributes.getValue("", "access").equals("public")) {
    				return false;
    			}
    			//its an EBI biosample, or has no accession, skip
    			if (attributes.getValue("", "accession") == null || attributes.getValue("", "accession").startsWith("SAME")) {
    				return false;
    			}
    			return true;
    		}
    		
    	};

		NCBIFTP ncbiftp = new NCBIFTP();
		ncbiftp.setup("ftp.ncbi.nlm.nih.gov");
		
		XMLFragmenter fragment;
		try {
			fragment = XMLFragmenter.newInstance();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		}
		
    	try {
			fragment.handleStream(new GZIPInputStream(ncbiftp.streamFromFTP("/biosample/biosample_set.xml.gz")), "UTF-8", callback);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	
    	return accessions;
    }
    
    private Set<String> getNCBIIDforAccession(Collection<String> accessionsSAMN, Map<String, Integer> samnNcbi) {
    	Set<String> ids = new HashSet<String>();
    	for (String accessionSAMN : accessionsSAMN) {
    		ids.add(samnNcbi.get(accessionSAMN).toString());
    	}
    	return ids;
    }
    
    private Set<String> getENASubmissionsForSAMEAccessions(Collection<String> accessionsSAME) {
		final Set<String> accessionsERA = new HashSet<String>();
		JdbcTemplate t = getENAJdbcTemplate();
		Object[] args = new Object[1];
		int[] types = new int[1];
		for (String accessionSAME : accessionsSAME) {
			args[0] = accessionSAME;
			types[0] = java.sql.Types.VARCHAR;
			accessionsERA.addAll(
					t.query("SELECT SUBMISSION_ID FROM SAMPLE WHERE BIOSAMPLE_ID = ?", args, types, getAccessionRowMapper()));
		}
		return accessionsERA;
    }

}
