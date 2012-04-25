package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

import junit.framework.TestCase;

public class TestCorrector  extends TestCase {

    private URL resource;
    private SampleTabParser<SampleData> parser;
    private Logger log = LoggerFactory.getLogger(getClass());

    public void setUp(){
        resource = getClass().getClassLoader().getResource("GEN-ERP001075/sampletab.pre.txt");
        parser = new SampleTabParser<SampleData>();
    }
    
    public void testSimple() {
        SampleData stA = null;
        SampleData stB = null;
        
        try {
            stA = parser.parse(resource.openStream());
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            fail();
            return;
        }
        Corrector corrector = new Corrector();
        corrector.correct(stA);

        for (SampleNode s : stA.scd.getNodes(SampleNode.class)){
            boolean hasDatabase = false;
            for (SCDNodeAttribute a : s.getAttributes()){
                if (DatabaseAttribute.class.isInstance(a)){
                    hasDatabase = true;
                }
            }
            assertTrue(hasDatabase);
        }

        SampleTabWriter w;
        StringWriter sw = new StringWriter();
        w = new SampleTabWriter(sw);
        try {
            w.write(stA);
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        System.out.println(sw.toString());
    }
}
