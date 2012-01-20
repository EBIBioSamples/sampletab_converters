package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import org.dom4j.DocumentException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

import junit.framework.TestCase;


public class TestNCBIBiosampleToSampleTab extends TestCase {
	
	private URL resource;
	
	private NCBIBiosampleToSampleTab converter;

    public void setUp() {
        resource = getClass().getClassLoader().getResource("ncbibiosample/2.xml");
        converter = NCBIBiosampleToSampleTab.getInstance();
    }

    public void tearDown() {
        resource = null;
        converter = null;
    }

    public void testConversion() {
    	SampleData st;
		try {
			st = converter.convert(resource.getFile());
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
		} catch (ParseException e) {
			e.printStackTrace();
            fail();
		} catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
			e.printStackTrace();
            fail();
		} catch (DocumentException e) {
			e.printStackTrace();
            fail();
		} catch (MalformedURLException e) {
            e.printStackTrace();
            fail();
        }
    }

}
