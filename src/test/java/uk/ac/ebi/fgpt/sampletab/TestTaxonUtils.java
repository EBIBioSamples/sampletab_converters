package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.TaxonUtils;
import junit.framework.TestCase;


public class TestTaxonUtils extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());

    public void testHuman() {
/*
 * Disabled for the moment because Bamboo cant seem to get its proxying sorted out.
        try {
            assertEquals("Homo sapiens", TaxonUtils.getTaononOfID(9606));
        } catch (DocumentException e) {
            e.printStackTrace();
            fail();
        }
        */
    }

}
