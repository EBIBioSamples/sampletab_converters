package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import junit.framework.TestCase;


public class TestIMSRTAbWebSummary extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());

    public void testIMSRTabWebSummary() {
        IMSRTabWebSummary sum = IMSRTabWebSummary.getInstance();

        try {
            sum.get();
        } catch (NumberFormatException e) {
            e.printStackTrace();
            fail();
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        
        assertNotNull(sum);
        assertTrue(sum.sites.size() > 0);
        assertTrue(sum.sites.contains("APB"));
        assertEquals(sum.sites.size(), sum.facilities.size());
        assertEquals(sum.sites.size(), sum.strainss.size());
        assertEquals(sum.sites.size(), sum.esliness.size());
        assertEquals(sum.sites.size(), sum.totals.size());
        assertEquals(sum.sites.size(), sum.updates.size());
    }

}
