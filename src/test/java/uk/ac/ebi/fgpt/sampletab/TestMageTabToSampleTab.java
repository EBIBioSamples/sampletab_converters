package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.mged.magetab.error.ErrorItem;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.arrayexpress.MageTabToSampleTab;


public class TestMageTabToSampleTab extends TestCase {
	
	private URL resource;
	
	private MageTabToSampleTab converter;
	
	private MAGETABParser<MAGETABInvestigation> mtparser;
    private List<ErrorItem> errorItems;

    public void setUp() {
        resource = getClass().getClassLoader().getResource("E-MEXP-986/E-MEXP-986.idf.txt");
        //this breaks limpopo
        //resource = getClass().getClassLoader().getResource("E-GEOD-20076/E-GEOD-20076.idf.txt");
        converter = new MageTabToSampleTab();
        mtparser = new MAGETABParser<MAGETABInvestigation>();
        errorItems = new ArrayList<ErrorItem>();
        mtparser.addErrorItemListener(new ErrorItemListener() {
            public void errorOccurred(ErrorItem item) {
                errorItems.add(item);
            }
        });
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
			
			assertTrue("Has at least one SCD node", st.scd.getAllNodes().size() > 0);
			
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
