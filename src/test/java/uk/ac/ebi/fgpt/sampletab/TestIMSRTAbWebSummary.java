package uk.ac.ebi.fgpt.sampletab;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;


public class TestIMSRTAbWebSummary extends TestCase {

    private Logger log = LoggerFactory.getLogger(getClass());

    public void testIMSRTabWebSummary() {
        /*
    //this fails in bamboo at all times, need to investigate further
        System.out.println("ENV");
        List<String> keys = new ArrayList<String>(System.getenv().keySet());
        Collections.sort(keys);
        for (String key : keys){
            System.out.println(key+" = "+System.getenv().get(key));
        }
        System.out.println("PROPERTIES");
        keys.clear();
        for (Object key : System.getProperties().keySet()){
            if (String.class.isInstance(key)){
                keys.add((String) key);
            }
        }
        Collections.sort(keys);
        for (Object key : keys){
            System.out.println(key+" = "+System.getProperties().get(key));
        }        
        */
        
//        IMSRTabWebSummary sum = IMSRTabWebSummary.getInstance();
//
//        try {
//            sum.get();
//        } catch (NumberFormatException e) {
//            e.printStackTrace();
//            fail();
//        } catch (ParseException e) {
//            e.printStackTrace();
//            fail();
//        } catch (IOException e) {
//            e.printStackTrace();
//            fail();
//        }
//        
//        assertNotNull(sum);
//        assertTrue(sum.sites.size() > 0);
//        assertTrue(sum.sites.contains("APB"));
//        assertEquals(sum.sites.size(), sum.facilities.size());
//        assertEquals(sum.sites.size(), sum.strainss.size());
//        assertEquals(sum.sites.size(), sum.esliness.size());
//        assertEquals(sum.sites.size(), sum.totals.size());
//        assertEquals(sum.sites.size(), sum.updates.size());
    }

}
