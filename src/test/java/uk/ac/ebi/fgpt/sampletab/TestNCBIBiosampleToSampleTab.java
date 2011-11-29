package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.text.ParseException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

import junit.framework.TestCase;


public class TestNCBIBiosampleToSampleTab extends TestCase {
	
	private URL resource;
	
	private NCBIBiosampleToSampleTab converter;

    public void setUp() {
        resource = getClass().getClassLoader().getResource("ncbibiosample/4.xml");
        converter = NCBIBiosampleToSampleTab.getInstance();
    }

    public void tearDown() {
        resource = null;
        converter = null;
    }

    public void testConversion() {
    	SampleData st;
		try {
			st = converter.convert(resource);
			//assertSame("Titles should match", st.msi.submissionTitle, "ATCC 43183");
			
			StringWriter out = new StringWriter();
			SampleTabWriter sampletabwriter = new SampleTabWriter(out);
			try {
				sampletabwriter.write(st);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			System.out.println(out.toString());
		} catch (SAXException e) {
			e.printStackTrace();
            fail();
		} catch (IOException e) {
			e.printStackTrace();
            fail();
		} catch (ParseException e) {
			e.printStackTrace();
            fail();
		}
    }

}
