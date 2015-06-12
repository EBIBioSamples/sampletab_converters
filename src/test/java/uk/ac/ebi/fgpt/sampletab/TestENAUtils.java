package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils.MissingBioSampleException;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils.UnrecognizedBioSampleException;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class TestENAUtils {

	
	private static Document submissionDoc;
	private static Document sampleDoc;
	private static Document studyDoc;
	
	public TestENAUtils() {
		
	}

	@BeforeClass
	public static void setupOnce() throws DocumentException, IOException {
		submissionDoc = XMLUtils.getDocument(new BufferedReader(new InputStreamReader(TestENAUtils.class.getResource("/ENA/ERA000073.xml").openStream())));
		sampleDoc = XMLUtils.getDocument(new BufferedReader(new InputStreamReader(TestENAUtils.class.getResource("/ENA/ERS000016.xml").openStream())));
		studyDoc = XMLUtils.getDocument(new BufferedReader(new InputStreamReader(TestENAUtils.class.getResource("/ENA/ERP000004.xml").openStream())));
	}
	
	
	@Test
	public void testGetBioSampleIdForSample() throws UnrecognizedBioSampleException, MissingBioSampleException {
		
		ENAUtils.getBioSampleIdForSample(XMLUtils.getChildByName(sampleDoc.getRootElement(), "SAMPLE"));
		
	}
	
}
