package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

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
			MAGETABInvestigation mt = mtparser.parse(resource);
			SampleData st = converter.convert(mt);
			assertSame("Titles should match", st.msi.submissionTitle, mt.IDF.investigationTitle);
			//assertSame("Titles should match", st.msi.submissionTitle, "Transcription profiling of wild type and DREB2C over-expression Arabidopsis plants");
			
			StringWriter out = new StringWriter();
			SampleTabWriter sampletabwriter = new SampleTabWriter(out);
			try {
				sampletabwriter.write(st);
			} catch (IOException e) {
				fail();
				e.printStackTrace();
			}
			System.out.println(out.toString());
		} catch (ParseException e) {
            e.printStackTrace();
            fail();
		} 
    }

}
