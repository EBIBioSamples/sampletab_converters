package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.net.URL;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;

import junit.framework.TestCase;


public class TestMageTabToSampleTab extends TestCase {
	
	private URL resource;
	
	private MageTabToSampleTab converter;
	
	private MAGETABParser<MAGETABInvestigation> mtparser;

    public void setUp() {
        resource = getClass().getClassLoader().getResource("E-MEXP-986/E-MEXP-986.idf.txt");
        converter = MageTabToSampleTab.getInstance();
        mtparser = new MAGETABParser<MAGETABInvestigation>();
    }

    public void tearDown() {
        resource = null;
        converter = null;
    }

    public void testConversion() {
    	try {
			SampleData st = converter.convert(resource);
			MAGETABInvestigation mt = mtparser.parse(resource);
			assertSame("Titles should match", st.msi.submissionTitle, mt.IDF.investigationTitle);
		} catch (ParseException e) {
            e.printStackTrace();
            fail();
		} catch (IOException e) {
            e.printStackTrace();
            fail();
		}
    }

}
