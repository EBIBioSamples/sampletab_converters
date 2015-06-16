package uk.ac.ebi.fgpt.sampletab;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

import org.junit.Test;
import org.xml.sax.SAXException;

import uk.ac.ebi.fgpt.sampletab.ncbi.NCBIFTPDriver;

public class TestNCBIupdateDownloader {
		
	@Test
	public void testXML() throws Exception {
		NCBIFTPDriver driver = new NCBIFTPDriver();
		driver.setup();
		
		InputStream inputStream = getClass().getResource("/ncbibiosample/sample.xml").openStream();		
		assertNotNull(inputStream);		
		driver.handleStream(inputStream);
	}
	@Test
	public void testGZXML() throws Exception {
		NCBIFTPDriver driver = new NCBIFTPDriver();
		driver.setup();
		
		InputStream inputStream = getClass().getResource("/ncbibiosample/sample.xml.gz").openStream();
		assertNotNull(inputStream);		
		driver.handleGZStream(inputStream);
	}
}
