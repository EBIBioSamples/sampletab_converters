package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;

import junit.framework.TestCase;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.EuroPMCUtils;


public class TestEuroPMCUtils extends TestCase {
    private Logger log = LoggerFactory.getLogger(getClass());

    public void testBioSD() {
        String title = null;
        
        try {
            title = EuroPMCUtils.getTitleByPUBMEDid(22096232);
        } catch (DocumentException e) {
            log.error("problem getting pubmedid", e);
            fail();
        } catch (IOException e) {
            log.error("problem getting pubmedid", e);
            fail();
        }
        assertEquals("The BioSample Database (BioSD) at the European Bioinformatics Institute.", title);

    }
}
