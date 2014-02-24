package uk.ac.ebi.fgpt.sampletab;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.BioSDUtils;


public class TestBioSDUtils extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());
    public void testENA() {
    /*
        Set<String> accessions = new HashSet<String>();
        accessions.add("SAMEA958157");
        try {
            assertEquals(accessions, BioSDUtils.getBioSDAccessionOf("ERS011203"));
        } catch (DocumentException e) {
            e.printStackTrace();
            fail();
        }*/
    }
    
    public void testIsAccessinPublic() {
        if (!BioSDUtils.isBioSDAccessionPublic("SAME123739")) {
            fail();
        }
        if (BioSDUtils.isBioSDAccessionPublic("failthis")) {
            fail();
        }
    }
}
